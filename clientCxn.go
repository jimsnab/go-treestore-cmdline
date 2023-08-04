package treestore_cmdline

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jimsnab/go-lane"
)

// The following client state machine progresses through the lifecycle
// of a client connection. A client processes only one command at a
// time.
const (
	csNone            cxnState = iota
	csInitialize               // can progress to csWaitForCommand or csTerminate
	csWaitForCommand           // can progress to csDispatchCommand or csTerminate
	csDispatchCommand          // can progress to csTerminate on an interruption, or csWaitForCommand after command processing is complete
	csTerminate                // closes the client
)

type (
	cxnState int

	// clientCxn holds state about the socket connection. It links
	// 1-to-1 to a clientState instance that is common to any type
	// of client connection.
	clientCxn struct {
		cs          *clientState
		started     time.Time
		mu          sync.Mutex // synchronizes access to waiting, closing flags
		cxn         net.Conn
		socketState cxnState
		csceCh      chan *clientStateEvent
		waiting     bool
		closing     bool
		inbound     []byte
		respVersion int
	}
)

func newClientCxn(l lane.Lane, cxn net.Conn, dispatcher *cmdDispatcher) *clientCxn {
	cc := &clientCxn{
		cxn:         cxn,
		started:     time.Now(),
		socketState: csNone,
		csceCh:      make(chan *clientStateEvent, 3),
	}

	cc.cs = newClientState(l, cc, dispatcher)

	cc.queueStateChange(csInitialize, nil)

	go cc.run()

	return cc
}

func (cc *clientCxn) ClientInfo() []string {
	since := time.Since(cc.started)
	return []string{
		"addr=" + cc.cxn.RemoteAddr().String(),
		"laddr=" + cc.cxn.LocalAddr().String(),
		"age=" + fmt.Sprintf("%d", int64(since.Seconds())),
	}
}

func (cc *clientCxn) MatchFilter(filter map[string]string) bool {
	for k, v := range filter {
		switch k {
		case "addr":
			str := cc.cxn.RemoteAddr().String()
			if v != str {
				return false
			}

		case "laddr":
			str := cc.cxn.LocalAddr().String()
			if v != str {
				return false
			}
		}
	}
	return true
}

func (cc *clientCxn) queueStateChange(newState cxnState, eventData any) {
	cc.csceCh <- &clientStateEvent{
		newState:  newState,
		eventData: eventData,
	}
}

// request connection close
func (cc *clientCxn) RequestClose() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if !cc.closing {
		cc.closing = true
		if cc.waiting {
			// in a blocking read, close the socket
			cc.cxn.Close()
		}
		cc.queueStateChange(csTerminate, nil)
	}
}

func (cc *clientCxn) IsCloseRequested() bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.closing
}

func requestAllCxnClose() {
	processAllClients(func(id int64, cs *clientState) {
		cc, ok := cs.client.(*clientCxn)
		if ok {
			cc.RequestClose()
		}
	})
}

func waitForAllCxnClose() {
	for {
		if !isClientActive() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (cc *clientCxn) run() {
	for {
		event := <-cc.csceCh

		cc.socketState = event.newState
		switch cc.socketState {
		case csInitialize:
			cc.onInitialize()
		case csTerminate:
			cc.onTerminate()
			cc.cs.l.Tracef("client %d at %s terminated", cc.cs.id, cc.cxn.RemoteAddr().String())
			return
		case csWaitForCommand:
			if cc.closing {
				cc.queueStateChange(csTerminate, nil)
			} else {
				cc.onWaitForCommand()
			}
		case csDispatchCommand:
			cc.onDispatchCommand(event.eventData.(rawRequest))
		}
	}
}

func (cc *clientCxn) onTerminate() {
	cc.cxn.Close()
	cc.cs.unregister()
}

func (cc *clientCxn) onInitialize() {
	cc.queueStateChange(csWaitForCommand, nil)
}

func (cc *clientCxn) onWaitForCommand() {
	buffer := make([]byte, 1024*8)

	cc.mu.Lock()
	cc.waiting = true
	cc.mu.Unlock()

	n, err := cc.cxn.Read(buffer)

	cc.mu.Lock()
	cc.waiting = false
	cc.mu.Unlock()

	if err != nil {
		if !errors.Is(err, io.EOF) {
			cc.cs.l.Debugf("read error from %s: %s", cc.cxn.RemoteAddr().String(), err)
		} else {
			cc.cs.l.Infof("client disconnected: %s", cc.cxn.RemoteAddr().String())
		}
		cc.queueStateChange(csTerminate, nil)
		return
	}

	if cc.inbound == nil {
		cc.inbound = buffer[0:n]
	} else {
		cc.inbound = append(cc.inbound, buffer[0:n]...)
	}

	cc.cs.l.Tracef("received %d bytes of command data from client", len(cc.inbound))

	cmd, length := cc.parseCommand()
	if length == 0 {
		cc.queueStateChange(csWaitForCommand, nil)
	} else if length > 0 {
		cc.inbound = cc.inbound[length:]
		cc.queueStateChange(csDispatchCommand, cmd)
	} else {
		cc.cs.l.Infof("malformed command sent from client - terminating")
		cc.queueStateChange(csTerminate, nil)
	}
}

func (cc *clientCxn) parseCommand() (req rawRequest, length int) {
	//
	// The stream format is:
	//
	// packetSize uint32 big endian
	// packet [packetSize]byte
	//
	// The packet is a command line with args separated with line breaks:
	//
	// "<cmdName>\n<first arg>\n<second arg>"
	//
	// Args have value escaping and will be unescaped. Value escaping is
	// simply \nn where nn is the hex byte value. Bytes < 32, > 127 and
	// 0x5C (backslash) must be escaped.
	//
	// For example, a set value command with a line break looks like this:
	//
	// fmt.Printf("setkv\n/some/value\nvalue having\\0Dtwo lines")
	//
	// 		setkv
	//		/some/value
	//      value having\0Dtwo lines
	//

	if len(cc.inbound) < 4 {
		return
	}

	packetSize := binary.BigEndian.Uint32(cc.inbound)
	if len(cc.inbound)-4 < int(packetSize) {
		return
	}

	packet := cc.inbound[4 : 4+packetSize]
	escapedArgs := bytes.Split(packet, []byte("\n"))

	req = rawRequest{
		exact: make([][]byte, 0, len(escapedArgs)),
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

	length = 4 + int(packetSize)
	return
}

func (cc *clientCxn) onDispatchCommand(cmd rawRequest) {
	go func() {
		response, err := cc.cs.dispatch(cmd)
		if err != nil {
			cc.cs.l.Debugf("dispatch error: %s", err)
			cc.cxn.Close()
			return
		}

		size := make([]byte, 4)
		binary.BigEndian.PutUint32(size, uint32(len(response)))

		n, err := cc.cxn.Write(size)
		if err != nil {
			cc.cs.l.Debugf("write error: %s", err)
			cc.cxn.Close()
			return
		}

		n, err = cc.cxn.Write(response)
		if err != nil {
			cc.cs.l.Debugf("write error: %s", err)
			cc.cxn.Close()
		} else {
			cc.cs.l.Tracef("wrote %d bytes", n)
			cc.queueStateChange(csWaitForCommand, nil)
		}
	}()
}

func (cc *clientCxn) ServerAddr() string {
	return cc.cxn.LocalAddr().String()
}

func (cc *clientCxn) ClientAddr() string {
	return cc.cxn.RemoteAddr().String()
}

func (cc *clientCxn) ServerNow() time.Time {
	return time.Now()
}
