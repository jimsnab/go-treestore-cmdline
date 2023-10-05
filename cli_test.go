package treestore_cmdline

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/jimsnab/go-treestore"
)

type (
	testClient struct {
		l       lane.Lane
		cxn     net.Conn
		inbound []byte
	}
)

func testSetup(t *testing.T) (tc *testClient) {
	l := lane.NewTestingLane(context.Background())
	//l = lane.NewLogLaneWithCR(context.Background())
	srv := NewTreeStoreCmdLineServer(l)
	srv.StartServer("localhost", 6771, "", 100, nil)

	t.Cleanup(func() {
		srv.StopServer()
		srv.WaitForTermination()
	})

	var cxn net.Conn
	cxn, err := net.Dial("tcp", "localhost:6771")
	if err != nil {
		t.Fatalf("can't connect: %s", err.Error())
		return
	}

	tc = &testClient{
		l:   l,
		cxn: cxn,
	}
	return
}

// Sends a raw command-line encoded command to the treestore server. This
// can be used to implement a CLI client.
func (tc *testClient) rawCommand(t *testing.T, args ...string) (response map[string]any) {
	//
	// Send the command with args separated by \n
	//
	// "setk\n/key/path\n"
	//

	joined := strings.Join(args, "\n")

	req := make([]byte, len(joined)+4)
	binary.BigEndian.PutUint32(req, uint32(len(joined)))
	copy(req[4:], []byte(joined))

	n, err := tc.cxn.Write(req)
	if err != nil {
		t.Fatalf("failed to write request: %s", err.Error())
		return
	}
	if n != len(req) {
		err = fmt.Errorf("%d bytes sent of %d", n, len(req))
		t.Fatalf("failed to write request: %s", err.Error())
		return
	}

	//
	// The response will be returned in json.
	//

	for {
		// buffer must be allocated for each read, because tc.inbound slice is referencing it
		buffer := make([]byte, 1024*8)

		// put a time limit on an api
		tc.cxn.SetReadDeadline(time.Now().Add(20 * time.Second))
		n, err = tc.cxn.Read(buffer)

		if err != nil {
			if !errors.Is(err, io.EOF) && !strings.HasSuffix(err.Error(), "use of closed network connection") {
				tc.l.Errorf("read error from %s: %s", tc.cxn.RemoteAddr().String(), err.Error())
			}
			t.Fatal(err)
			return
		}

		if tc.inbound == nil {
			tc.inbound = buffer[0:n]
		} else {
			tc.inbound = append(tc.inbound, buffer[0:n]...)
		}

		tc.l.Tracef("received %d bytes from server", len(tc.inbound))

		var length int
		length, response, err = tc.parseResponse()
		if err != nil {
			t.Fatalf("bad response from %s: %s", tc.cxn.RemoteAddr().String(), err.Error())
			return
		}
		if response != nil {
			tc.inbound = tc.inbound[length:]

			errText, isError := response["error"].(string)
			if isError {
				err = errors.New(errText)
				return
			}
			return
		}
	}
}

func (tc *testClient) parseResponse() (length int, response map[string]any, err error) {
	if len(tc.inbound) < 4 {
		return
	}

	packetSize := binary.BigEndian.Uint32(tc.inbound)
	if len(tc.inbound)-4 < int(packetSize) {
		tc.l.Tracef("insufficient input, expecting %d bytes, have %d bytes", packetSize, len(tc.inbound)-4)
		return
	}

	packet := tc.inbound[4 : 4+packetSize]
	if err = json.Unmarshal(packet, &response); err != nil {
		return
	}

	length = 4 + int(packetSize)
	return
}

func resultAddress(t *testing.T, res map[string]any, field string) treestore.StoreAddress {
	val, exists := res[field].(float64)
	if !exists {
		t.Fatalf("%s does not exist", field)
		return 0
	}

	return treestore.StoreAddress(val)
}

func resultBool(t *testing.T, res map[string]any, field string) bool {
	val, exists := res[field].(bool)
	if !exists {
		t.Fatalf("%s does not exist", field)
		return false
	}

	return val
}

func resultStrArray(t *testing.T, res map[string]any, field string) []string {
	val, exists := res[field].([]any)
	if !exists {
		t.Fatalf("%s does not exist", field)
		return nil
	}

	strs := make([]string, 0, len(val))
	for _, v := range val {
		str, is := v.(string)
		if !is {
			t.Fatalf("%s array element is not a string", field)
			return nil
		}
		strs = append(strs, str)
	}

	return strs
}

func TestSetGetK(t *testing.T) {
	tc := testSetup(t)

	sk := treestore.MakeStoreKey("client", "test", "key")

	res := tc.rawCommand(t, "setk", string(sk.Path))

	if resultAddress(t, res, "address") != 4 || resultBool(t, res, "exists") {
		t.Error("unexpected result")
	}
}

func TestKeys(t *testing.T) {
	tc := testSetup(t)

	sk1 := treestore.MakeStoreKey("client", "test", "key")
	sk2 := treestore.MakeStoreKey("client", "test", "data", "cat")
	sk3 := treestore.MakeStoreKey("client", "test", "data", "mouse")

	tc.rawCommand(t, "setk", string(sk1.Path))
	tc.rawCommand(t, "setk", string(sk2.Path))
	tc.rawCommand(t, "setk", string(sk3.Path))

	res := tc.rawCommand(t, "keys", "/client/**")

	keys := resultStrArray(t, res, "matches")
	if len(keys) != 3 {
		t.Fatal("wrong key count")
	}
	if keys[0] != "test/data/cat" || keys[1] != "test/data/mouse" || keys[2] != "test/key" {
		t.Fatal("unexpected keys")
	}
}
