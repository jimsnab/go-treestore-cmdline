package treestore_cmdline

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jimsnab/go-lane"
)

type (
	mainEngine struct {
		mu              sync.Mutex
		started         bool
		l               lane.Lane
		tss             *treeStoreSet
		server          net.Listener
		cxns            []net.Conn
		exitSaver       chan struct{}
		saverTerminated chan struct{}
		canExit         chan struct{}
		terminating     bool
		port            int
		iface           string
		dispatcher      *cmdDispatcher
		directCs        *clientState
	}

	TreeStoreCmdLineServer interface {
		// Starts a socket server using the specified network interface and port, maintaining
		// state in persistPath.
		//
		// If endpoint is "", the server will listen on all network interfaces.
		// If port is 0, the server will listen on port 6770.
		// If persistPath is "", data will be maintained in memory only.
		//
		// persistPath specifies the base file name; each database name plus ".db" will
		// be appended to this base.
		//
		// The commands sent to the server require the request format:
		//
		// <length> "<cmdname>\n<arg>\n<arg>\n"
		//
		// Where <length> is big-endian 32-bit length of the command line string.
		// <arg> must be value-escaped, which means any byte < 32 must be sent as
		// \xx (backslash and two character hex); and the backslash must be sent
		// as \5C.
		//
		// A request key path must be path-escaped (\s for forward slash and \S for
		// backslash), before being value-escaped. A key with a forward slash such
		// as "first/second" is placed on the wire like this example:
		//
		// <length> "setk\nfirst\\5Cssecond\n"
		//
		// e.g. 00 00 00 15 73 65 74 6B 0A 66 69 72 73 74 5C 35 43 73 73 65 63 6F 6E 64 0A
		//      <length 21> s  e  t  k  \n f  i  r  s  t  \  5  C  s  s  e  c  o  n  d  \n
		//
		// The response is a JSON structure, sent as:
		//
		// <length> "<json>"
		//
		// In the JSON response, key paths will be path-escaped, and response values
		// will be value-escaped.
		StartServer(endpoint string, port int, persistPath string, appVersion int, opLog OpLogHandler) error

		// Initiates server termination, if it is running.
		StopServer() error

		// Waits for the server to stop
		WaitForTermination()

		// Returns the server address
		ServerAddr() string

		// Send a raw command
		Dispatch(lines [][]byte) (reply []byte, err error)
	}
)

func NewTreeStoreCmdLineServer(l lane.Lane) TreeStoreCmdLineServer {
	eng := mainEngine{
		l:    l,
		cxns: []net.Conn{},
	}
	return &eng
}

func (eng *mainEngine) StartServer(endpoint string, port int, persistPath string, appVersion int, opLog OpLogHandler) error {
	eng.mu.Lock()
	defer eng.mu.Unlock()

	if eng.started {
		return fmt.Errorf("already started")
	}

	if port != 0 {
		eng.port = port
	} else {
		eng.port = 6770
	}

	if endpoint != "" {
		eng.iface = endpoint
	}

	tss, err := newTreeStoreSet(eng.l, persistPath, appVersion)
	if err != nil {
		return err
	}
	eng.tss = tss

	// launch termination monitiors
	eng.canExit = make(chan struct{})

	// launch periodic save goroutine
	eng.periodicSave()

	// start accepting connections and processing them
	err = eng.startServer(opLog)
	if err != nil {
		return err
	}
	eng.started = true

	return nil
}

func (eng *mainEngine) StopServer() error {
	// ensure only one termination
	eng.mu.Lock()
	if !eng.started {
		eng.mu.Unlock()
		return fmt.Errorf("not started")
	}

	isTerminating := eng.terminating
	eng.terminating = true
	eng.mu.Unlock()

	if !isTerminating {
		go func() { eng.onTerminate() }()
	}

	return nil
}

func (eng *mainEngine) onTerminate() {
	if eng.server != nil {
		// close the server and wait for all active connections to finish
		eng.l.Tracef("closing server")
		eng.server.Close()

		eng.mu.Lock()
		for _, cxn := range eng.cxns {
			eng.l.Tracef("closing connection %s <-> %s", cxn.LocalAddr().String(), cxn.RemoteAddr().String())
			cxn.Close()
		}
		eng.cxns = []net.Conn{}
		eng.mu.Unlock()

		eng.l.Infof("waiting for any open request connections to complete")
		requestAllCxnClose()
		waitForAllCxnClose()
		eng.l.Infof("termination of %s completed", eng.server.Addr().String())
	}

	// stop the periodic saver (if running)
	if eng.exitSaver != nil {
		eng.l.Tracef("closing database saver")
		eng.exitSaver <- struct{}{}
		<-eng.saverTerminated
		eng.l.Tracef("database saver closed")
	}

	eng.canExit <- struct{}{}
}

func (eng *mainEngine) periodicSave() {
	// make a periodic save that will also ensure save upon termination
	if eng.tss.basePath != "" {
		eng.exitSaver = make(chan struct{})
		eng.saverTerminated = make(chan struct{})
		go func() {
			timer := time.NewTicker(time.Second)
			for {
				select {
				case <-eng.exitSaver:
					eng.l.Trace("saver loop is exiting")
					timer.Stop()
					eng.tss.save(eng.l)
					eng.saverTerminated <- struct{}{}
					return
				case <-timer.C:
					eng.tss.save(eng.l)
				}
			}
		}()
	}
}

func (eng *mainEngine) startServer(opLog OpLogHandler) error {
	// establish socket service
	var err error

	if eng.iface == "" {
		eng.iface = fmt.Sprintf(":%d", eng.port)
	} else {
		eng.iface = fmt.Sprintf("%s:%d", eng.iface, eng.port)
	}

	eng.server, err = net.Listen("tcp", eng.iface)
	if err != nil {
		eng.l.Errorf("error listening: %s", err.Error())
		return err
	}
	eng.l.Infof("listening on %s", eng.server.Addr().String())

	eng.dispatcher = newCmdDispatcher(eng.port, eng.iface, eng.tss, opLog)

	go func() {
		// accept connections and process commands
		for {
			connection, err := eng.server.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					eng.l.Errorf("accept error: %s", err)
				}
				break
			}
			eng.mu.Lock()
			eng.cxns = append(eng.cxns, connection)
			eng.mu.Unlock()
			eng.l.Infof("client connected: %s", connection.RemoteAddr().String())
			newClientCxn(eng.l, connection, eng.dispatcher)
		}
	}()

	return nil
}

func (eng *mainEngine) WaitForTermination() {
	// wait for server to quiesque
	<-eng.canExit
	eng.l.Info("finished serving requests")
}

func (eng *mainEngine) ServerAddr() string {
	eng.mu.Lock()
	defer eng.mu.Unlock()

	if eng.server == nil {
		return ""
	}

	return eng.server.Addr().String()
}

func (eng *mainEngine) Dispatch(escapedArgs [][]byte) (reply []byte, err error) {
	if eng.server == nil || eng.dispatcher == nil {
		err = errors.New("server not running")
		return
	}

	if eng.directCs == nil {
		cc := &clientCxn{
			cxn:         nil,
			started:     time.Now(),
			socketState: csNone,
			csceCh:      make(chan *clientStateEvent, 3),
		}

		eng.directCs = newClientState(eng.l, cc, eng.dispatcher)
	}

	req := rawRequest{
		exact: escapedArgs,
		args:  make([]string, 0, len(escapedArgs)),
	}

	for _, escapedArg := range escapedArgs {
		req.args = append(req.args, string(escapedArg))
		if !bytes.Contains(escapedArg, []byte("\\")) {
			req.exact = append(req.exact, escapedArg)
		} else {
			req.exact = append(req.exact, valueUnescape(string(escapedArg)))
		}
	}

	return eng.dispatcher.dispatchHandler(eng.l, eng.directCs, req)
}
