package om

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/devotionalist/open-message-client-go/wire"
)

// Config for the application-facing SDK.
type Config struct {
	ServiceURL     string // host:port of Proxy (prod) or Broker (dev)
	AuthToken      string
	MaxPending     int
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	MaxRetries     int
	ClientVersion  string
}

func (c *Config) defaults() {
	if c.MaxPending == 0 {
		c.MaxPending = 1000
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 10 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 8
	}
	if c.ClientVersion == "" {
		c.ClientVersion = "open-message-client-go/0.1"
	}
}

// Client is the open-message SDK entrypoint.
type Client struct {
	cfg    Config
	conn   net.Conn
	mu     sync.Mutex
	req    atomic.Uint64
	wait   map[uint64]chan wire.Frame
	inbox  chan wire.Frame
	closed chan struct{}
}

// Connect dials ServiceURL and completes Connect handshake.
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	cfg.defaults()
	d := net.Dialer{Timeout: cfg.DialTimeout}
	conn, err := d.DialContext(ctx, "tcp", cfg.ServiceURL)
	if err != nil {
		return nil, err
	}
	c := &Client{
		cfg: cfg, conn: conn,
		wait:   map[uint64]chan wire.Frame{},
		inbox:  make(chan wire.Frame, 256),
		closed: make(chan struct{}),
	}
	go c.readLoop()
	body, _ := wire.EncodeBody(wire.ConnectBody{
		ProtocolVersion: 1,
		ClientVersion:   cfg.ClientVersion,
		AuthToken:       cfg.AuthToken,
	})
	fr, err := c.roundTrip(ctx, wire.Header{Type: wire.TypeConnect, Body: body}, nil)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	if fr.Header.BrokerErr == wire.AuthFailed {
		_ = c.Close()
		return nil, errors.New("auth failed")
	}
	return c, nil
}

func (c *Client) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return c.conn.Close()
}

func (c *Client) readLoop() {
	for {
		fr, err := wire.Decode(c.conn)
		if err != nil {
			return
		}
		if fr.Header.Type == wire.TypeMessage {
			select {
			case c.inbox <- fr:
			case <-c.closed:
				return
			}
			continue
		}
		c.mu.Lock()
		ch, ok := c.wait[fr.Header.RequestID]
		c.mu.Unlock()
		if ok {
			select {
			case ch <- fr:
			default:
			}
		}
	}
}

func (c *Client) roundTrip(ctx context.Context, h wire.Header, payload []byte) (wire.Frame, error) {
	id := c.req.Add(1)
	h.RequestID = id
	ch := make(chan wire.Frame, 1)
	c.mu.Lock()
	c.wait[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.wait, id)
		c.mu.Unlock()
	}()
	buf, err := wire.Encode(h, payload)
	if err != nil {
		return wire.Frame{}, err
	}
	c.mu.Lock()
	_, err = c.conn.Write(buf)
	c.mu.Unlock()
	if err != nil {
		return wire.Frame{}, err
	}
	timer := time.NewTimer(c.cfg.RequestTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return wire.Frame{}, ctx.Err()
	case <-timer.C:
		return wire.Frame{}, context.DeadlineExceeded
	case fr := <-ch:
		return fr, nil
	}
}

func (c *Client) Lookup(ctx context.Context, topic string) (string, error) {
	body, _ := wire.EncodeBody(wire.LookupBody{Topic: topic})
	fr, err := c.roundTrip(ctx, wire.Header{Type: wire.TypeLookup, Body: body}, nil)
	if err != nil {
		return "", err
	}
	rb, err := wire.DecodeBody[wire.LookupResponseBody](fr.Header)
	return rb.BrokerURL, err
}

func (c *Client) reconnect(ctx context.Context, addr string) error {
	d := net.Dialer{Timeout: c.cfg.DialTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	c.mu.Lock()
	_ = c.conn.Close()
	c.conn = conn
	c.cfg.ServiceURL = addr
	c.mu.Unlock()
	go c.readLoop()
	body, _ := wire.EncodeBody(wire.ConnectBody{
		ProtocolVersion: 1,
		ClientVersion:   c.cfg.ClientVersion,
		AuthToken:       c.cfg.AuthToken,
	})
	_, err = c.roundTrip(ctx, wire.Header{Type: wire.TypeConnect, Body: body}, nil)
	return err
}

// Producer sends messages with optional idempotency.
type Producer struct {
	c          *Client
	topic      string
	name       string
	id         uint64
	idempotent bool
	seq        atomic.Uint64
	pending    chan struct{}
}

func (c *Client) CreateProducer(ctx context.Context, topic, name string, idempotent bool) (*Producer, error) {
	var last error
	for i := 0; i < c.cfg.MaxRetries; i++ {
		body, _ := wire.EncodeBody(wire.ProducerBody{Topic: topic, ProducerName: name, EnableIdempotency: idempotent})
		fr, err := c.roundTrip(ctx, wire.Header{Type: wire.TypeProducer, Body: body}, nil)
		if err != nil {
			last = err
			time.Sleep(time.Duration(i+1) * 50 * time.Millisecond)
			continue
		}
		if fr.Header.BrokerErr.Retryable() {
			last = fmt.Errorf("%s", fr.Header.BrokerErr)
			if fr.Header.BrokerErr == wire.NotOwner {
				if url, lerr := c.Lookup(ctx, topic); lerr == nil && url != "" && url != c.cfg.ServiceURL {
					_ = c.reconnect(ctx, url)
				}
			}
			time.Sleep(time.Duration(i+1) * 50 * time.Millisecond)
			continue
		}
		if fr.Header.BrokerErr != "" && fr.Header.BrokerErr != wire.OK {
			return nil, fmt.Errorf("producer: %s", fr.Header.BrokerErr)
		}
		sb, _ := wire.DecodeBody[wire.ProducerSuccessBody](fr.Header)
		return &Producer{
			c: c, topic: topic, name: name, id: sb.ProducerID,
			idempotent: idempotent, pending: make(chan struct{}, c.cfg.MaxPending),
		}, nil
	}
	return nil, last
}

func (p *Producer) Send(ctx context.Context, payload []byte) (ledgerID, entryID uint64, err error) {
	select {
	case p.pending <- struct{}{}:
		defer func() { <-p.pending }()
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
	seq := p.seq.Add(1)
	var last error
	for i := 0; i < p.c.cfg.MaxRetries; i++ {
		body, _ := wire.EncodeBody(wire.SendBody{ProducerID: p.id, SequenceID: seq})
		fr, err := p.c.roundTrip(ctx, wire.Header{Type: wire.TypeSend, Body: body}, payload)
		if err != nil {
			last = err
			time.Sleep(time.Duration(i+1) * 50 * time.Millisecond)
			continue
		}
		if fr.Header.Type == wire.TypeSendReceipt {
			rb, _ := wire.DecodeBody[wire.SendReceiptBody](fr.Header)
			return rb.LedgerID, rb.EntryID, nil
		}
		eb, _ := wire.DecodeBody[wire.SendErrorBody](fr.Header)
		if eb.Code.Retryable() || fr.Header.BrokerErr.Retryable() {
			last = fmt.Errorf("%s: %s", eb.Code, eb.Message)
			if eb.Code == wire.NotOwner || fr.Header.BrokerErr == wire.NotOwner {
				if url, lerr := p.c.Lookup(ctx, p.topic); lerr == nil && url != "" {
					_ = p.c.reconnect(ctx, url)
					np, perr := p.c.CreateProducer(ctx, p.topic, p.name, p.idempotent)
					if perr == nil {
						p.id = np.id
					}
				}
			}
			time.Sleep(time.Duration(i+1) * 50 * time.Millisecond)
			continue
		}
		return 0, 0, fmt.Errorf("send: %s %s", eb.Code, eb.Message)
	}
	return 0, 0, last
}

// Consumer receives messages with Flow permits.
type Consumer struct {
	c     *Client
	id    uint64
	topic string
	sub   string
}

func (c *Client) Subscribe(ctx context.Context, topic, sub, mode string) (*Consumer, error) {
	if mode == "" {
		mode = "Exclusive"
	}
	var last error
	for i := 0; i < c.cfg.MaxRetries; i++ {
		body, _ := wire.EncodeBody(wire.SubscribeBody{
			Topic: topic, Subscription: sub, Mode: mode, InitialPosition: "Earliest",
		})
		fr, err := c.roundTrip(ctx, wire.Header{Type: wire.TypeSubscribe, Body: body}, nil)
		if err != nil {
			last = err
			time.Sleep(time.Duration(i+1) * 50 * time.Millisecond)
			continue
		}
		if fr.Header.BrokerErr.Retryable() {
			last = fmt.Errorf("subscribe: %s", fr.Header.BrokerErr)
			if fr.Header.BrokerErr == wire.NotOwner {
				if url, lerr := c.Lookup(ctx, topic); lerr == nil && url != "" && url != c.cfg.ServiceURL {
					_ = c.reconnect(ctx, url)
				}
			}
			time.Sleep(time.Duration(i+1) * 50 * time.Millisecond)
			continue
		}
		if fr.Header.BrokerErr != "" && fr.Header.BrokerErr != wire.OK {
			return nil, fmt.Errorf("subscribe: %s", fr.Header.BrokerErr)
		}
		sb, _ := wire.DecodeBody[wire.SubscribeSuccessBody](fr.Header)
		return &Consumer{c: c, id: sb.ConsumerID, topic: topic, sub: sub}, nil
	}
	return nil, last
}

func (co *Consumer) Flow(ctx context.Context, permits int32) error {
	_ = ctx
	body, _ := wire.EncodeBody(wire.FlowBody{ConsumerID: co.id, Permits: permits})
	buf, err := wire.Encode(wire.Header{Type: wire.TypeFlow, RequestID: co.c.req.Add(1), Body: body}, nil)
	if err != nil {
		return err
	}
	co.c.mu.Lock()
	_, err = co.c.conn.Write(buf)
	co.c.mu.Unlock()
	return err
}

// Message is a delivered payload with MessageID (ledgerId, entryId).
type Message struct {
	LedgerID uint64
	EntryID  uint64
	Payload  []byte
}

func (co *Consumer) Receive(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return Message{}, ctx.Err()
	case fr := <-co.c.inbox:
		mb, _ := wire.DecodeBody[wire.MessageBody](fr.Header)
		return Message{LedgerID: mb.LedgerID, EntryID: mb.EntryID, Payload: fr.Payload}, nil
	}
}

func (co *Consumer) AckCumulative(ctx context.Context, ledgerID uint64, entryID int64) error {
	body, _ := wire.EncodeBody(wire.AckBody{ConsumerID: co.id, Cumulative: &struct {
		LedgerID uint64 `json:"ledgerId"`
		EntryID  int64  `json:"entryId"`
	}{LedgerID: ledgerID, EntryID: entryID}})
	_, err := co.c.roundTrip(ctx, wire.Header{Type: wire.TypeAck, Body: body}, nil)
	return err
}
