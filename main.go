package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/jimsnab/go-cmdline"
	"github.com/jimsnab/go-lane"
	"golang.org/x/term"
)

type (
	mainEngine struct {
		mu              sync.Mutex
		args            cmdline.Values
		l               lane.Lane
		tss             *treeStoreSet
		server          net.Listener
		exitSaver       chan struct{}
		saverTerminated chan struct{}
		canExit         chan struct{}
		terminating     bool
		port            int
		iface           string
	}
)


func main() {
	cl := cmdline.NewCommandLine()

	cl.RegisterCommand(
		mainHandler,
		"~ [<string-file>]?Runs a simple TreeStore server. Specify <file> to persist data to disk.",
		"[--trace]?Enable trace logging",
		"[--port <int-port>]?Specify the TCP port to listen on. The default is 6770.",
		"[--endpoint <string-interface>]?Specify the network interface to listen on. The default is all network interfaces.",
	)

	args := os.Args[1:] // exclude executable name in os.Args[0]
	err := cl.Process(args)
	if err != nil {
		cl.Help(err, "go-treestore-server", args)
	}
}

func mainHandler(args cmdline.Values) error {
	eng := mainEngine{args: args}

	eng.start()
	eng.waitForTermination()

	return nil
}

func (eng *mainEngine) start() {
	eng.l = lane.NewLogLane(context.Background())

	fmt.Printf("\n\nTreeStore server is now running\n\nPress any key to quit\n\n")

	isTrace := eng.args["--trace"].(bool)
	if !isTrace {
		eng.l.SetLogLevel(lane.LogLevelInfo)
	}

	port := eng.args["port"].(int)
	if port != 0 {
		eng.port = port
	} else {
		eng.port = 6770
	}

	iface := eng.args["interface"].(string)
	if iface != "" {
		eng.iface = iface
	}

	basePath := eng.args["file"].(string)
	eng.tss = newTreeStoreSet(eng.l, basePath)

	// launch termination monitiors
	eng.canExit = make(chan struct{})
	eng.killSignalMonitor()
	eng.exitKeyMonitor()

	// launch periodic save goroutine
	eng.periodicSave()

	// start accepting connections and processing them
	eng.startServer()
}

func (eng *mainEngine) startTermination() {
	// ensure only one termination
	eng.mu.Lock()
	isTerminating := eng.terminating
	eng.terminating = true
	eng.mu.Unlock()

	if isTerminating {
		return
	}

	go func() { eng.onTerminate() }()
}

func (eng *mainEngine) onTerminate() {
	if eng.server != nil {
		// close the server and wait for all active connections to finish
		eng.l.Tracef("closing server")
		eng.server.Close()

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

func (eng *mainEngine) killSignalMonitor() {
	// register a graceful termination handler
	sigs := make(chan os.Signal, 10)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		sig := <-sigs
		eng.l.Infof("termination %s signaled for %s", sig, eng.server.Addr().String())
		eng.startTermination()
	}()
}

func (eng *mainEngine) exitKeyMonitor() {
	// Start a go routine to detect a keypress. Upon termination
	// triggered another way, this goroutine will leak. Go does
	// not give a reasonable way to cancel a blocking I/O call.
	go func() {
		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println(err)
			return
		}
		defer term.Restore(int(os.Stdin.Fd()), oldState)

		b := make([]byte, 1)
		_, err = os.Stdin.Read(b)
		if err == nil {
			eng.startTermination()
		}
	}()
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

func (eng *mainEngine) startServer() {
	// establish socket service
	var err error

	if eng.iface == "" {
		eng.iface = fmt.Sprintf(":%d", eng.port)
	} else {
		eng.iface = fmt.Sprintf("%s:%d", eng.iface, eng.port)
	}
	eng.server, err = net.Listen("tcp", eng.iface)
	if err != nil {
		fmt.Println("Error listening: ", err.Error())
		os.Exit(1)
	}
	eng.l.Infof("listening on %s\r", eng.server.Addr().String())

	dispatcher := newCmdDispatcher(eng.port, eng.iface, eng.tss)

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
			eng.l.Infof("client connected: %s", connection.RemoteAddr().String())
			newClientCxn(eng.l, connection, dispatcher)
		}
	}()
}

func (eng *mainEngine) waitForTermination() {
	// wait for server to quiesque
	<-eng.canExit
	eng.l.Info("finished serving requests")
}
