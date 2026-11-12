package wire_test

import (
	"bytes"
	"testing"

	"github.com/devotionalist/open-message-client-go/wire"
)

func TestFrameRoundTrip(t *testing.T) {
	body, _ := wire.EncodeBody(wire.ConnectBody{ProtocolVersion: 1, ClientVersion: "test"})
	buf, err := wire.Encode(wire.Header{Type: wire.TypeConnect, RequestID: 7, Body: body}, []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	fr, err := wire.Decode(bytes.NewReader(buf))
	if err != nil {
		t.Fatal(err)
	}
	if fr.Header.Type != wire.TypeConnect || fr.Header.RequestID != 7 {
		t.Fatalf("%+v", fr.Header)
	}
	if string(fr.Payload) != "hi" {
		t.Fatalf("payload %q", fr.Payload)
	}
	cb, err := wire.DecodeBody[wire.ConnectBody](fr.Header)
	if err != nil || cb.ClientVersion != "test" {
		t.Fatalf("%v %+v", err, cb)
	}
}
