package treestore_cmdline

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/jimsnab/go-treestore"
)

const (
	CS_UNCAPTURED = iota
	CS_DRAINING
	CS_CAPTURED
	CS_CHECKING
)

type (
	unblockReason struct {
		reason  string
		isError bool
	}

	watchKey struct {
		ts  *treestore.TreeStore
		key string
	}

	// clientState holds all state associated with processing commands. A
	// client processes one command at a time.
	clientState struct {
		l               lane.Lane
		mu              sync.Mutex
		tss             *treeStoreSet
		selectedDb      string
		ts              *treestore.TreeStore
		id              int64
		name            string
		user            string
		client          TreeStoreClient
		disp            *cmdDispatcher
		cmdQueue        *[]*cmdContext
		watches         map[watchKey]uint64
		blocked         int32
		unblockPending  int32
		unblockCh       chan unblockReason
		respVersion     int
		noEvict         bool
		multiInProgress bool
	}
)

var clientId int64
var clientsMu sync.Mutex
var clients = map[int64]*clientState{}

func newClientState(l lane.Lane, client TreeStoreClient, dispatcher *cmdDispatcher) *clientState {
	cs := &clientState{
		l:           l,
		user:        "default",
		client:      client,
		disp:        dispatcher,
		tss:         dispatcher.tss,
		respVersion: 2,
		unblockCh:   make(chan unblockReason, 1),
		watches:     map[watchKey]uint64{},
	}

	cs.ts, _ = cs.tss.getDb(l, "main", true)

	clientsMu.Lock()
	defer clientsMu.Unlock()
	clientId++
	cs.id = clientId
	clients[clientId] = cs

	return cs
}

func isClientActive() bool {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	return len(clients) > 0
}

func processAllClients(op func(id int64, cs *clientState)) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	for id, cs := range clients {
		if !cs.client.IsCloseRequested() {
			op(id, cs)
		}
	}
}

func (cs *clientState) unregister() {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	delete(clients, cs.id)
}

func (cs *clientState) setLock(from, to int32) {
	us := time.Microsecond
	for {
		if atomic.CompareAndSwapInt32(&cs.blocked, from, to) {
			return
		}
		if us < 4000*time.Microsecond {
			us *= 2
		} else {
			us = time.Microsecond * time.Duration(rand.Intn(200))
		}
		time.Sleep(us)
	}
}

// Captures the client state in order to wait for data-ready event, a timeout,
// or an explicit unblock signal. A client can be captured by one blocking command.
// Additional commands cannot be processed until the blocking command completes.
func (cs *clientState) capture() chan unblockReason {

	// When a command such as BLMOVE needs to block, it "captures" the client.
	//
	// This operation might collide with other goroutines calling cs.unblock(),
	// so in that infrequent case, some looping occurs within cs.setLock().

	cs.setLock(CS_UNCAPTURED, CS_CAPTURED)
	return cs.unblockCh
}

// Releases the client state capture after successful receipt of the unblock
// signal. After releasing the capture, the non-blocking command processing
// continues until the command completes.
func (cs *clientState) releaseCapture() {

	// A blocked command can become unblocked in these ways:
	//
	//  1. Ordinary command completion (the command 'ready' channel gets an item)
	//  2. Command timeout (the timer's channel gets an item)
	//  3. Explicit unblock, due to connection loss or direct request (the
	//     unblock channel gets an item)
	//
	// select is issued to wait for the first item from one of those three
	// channels. After select completes, the content of the other channels
	// must be buffered to prevent incorrect goroutine blockage, and the
	// extra channel items must be ignored and discarded.
	//
	// Because the unlock channel is used by many commands, it must be
	// drained here before fully releasing the capture. The other two
	// channels exist only per command and will not be used again.

	// move to CS_DRAINING to prevent a new cs.unblockCh item in the middle of draining
	cs.setLock(CS_CAPTURED, CS_DRAINING)

	func() {
		for {
			select {
			case <-cs.unblockCh:
				// ignore and discard
			default:
				// empty - done
				return
			}
		}
	}()

	// drained - clear unblock state and release the capture
	atomic.StoreInt32(&cs.unblockPending, 0)
	cs.setLock(CS_DRAINING, CS_UNCAPTURED)
}

// Tells a blocking command (if any) to end with a timeout or error.
// For a timeout, pass reason as an empty string and isError false.
func (cs *clientState) unblock(reason string, isError bool) {
	us := time.Microsecond

	for {
		// N.B., checking is allowed in the midst of capture and release
		locked := atomic.SwapInt32(&cs.blocked, CS_CHECKING)
		if locked == CS_CAPTURED {
			// client is probably in select waiting for the unblock
			if atomic.CompareAndSwapInt32(&cs.unblockPending, 0, 1) {
				// only one unblock is posted per capture to prevent
				// getting stuck here
				cs.unblockCh <- unblockReason{reason: reason, isError: isError}
			}
		}
		atomic.SwapInt32(&cs.blocked, locked)

		if locked == CS_UNCAPTURED || locked == CS_CAPTURED {
			return
		}

		// CS_DRAINING, or CS_CHECKING from another goroutine, try again
		if us < 4000*time.Microsecond {
			us *= 2
		} else {
			us = time.Microsecond * time.Duration(rand.Intn(200))
		}
		time.Sleep(us)
	}
}

func (cs *clientState) isBlocked() bool {
	us := time.Microsecond

	for {
		blocked := false

		// N.B., checking is allowed in the midst of capture and release
		locked := atomic.SwapInt32(&cs.blocked, CS_CHECKING)
		if locked == CS_CAPTURED {
			blocked = true
		}
		atomic.SwapInt32(&cs.blocked, locked)

		if locked == CS_UNCAPTURED || locked == CS_CAPTURED {
			return blocked
		}

		// CS_DRAINING, or CS_CHECKING from another goroutine, try again
		if us < 4000*time.Microsecond {
			us *= 2
		} else {
			us = time.Microsecond * time.Duration(rand.Intn(200))
		}
		time.Sleep(us)
	}
}

func (cs *clientState) dispatch(req rawRequest) (output []byte, err error) {
	return cs.disp.dispatchHandler(cs.l, cs, req)
}

func (cs *clientState) setMultiInProgress(inProgress bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.multiInProgress = inProgress
}

func (cs *clientState) isMultiInProgress() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.multiInProgress
}

func (cs *clientState) selectDb(index string, create bool) (priorSelection string, valid bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	priorSelection = cs.selectedDb
	ts, valid := cs.tss.getDb(cs.l, index, true)
	if !valid {
		return
	}

	cs.selectedDb = index
	cs.ts = ts
	return
}
