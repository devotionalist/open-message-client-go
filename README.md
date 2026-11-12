# open-message-client-go

open-message official **Go** client (OM binary framing over TCP).

Sibling SDKs: [java](../open-message-client-java) · [rust](../open-message-client-rust) · [typescript](../open-message-client-typescript).

Protocol: [open-message/docs/design/21-wire-protocol.md](../open-message/docs/design/21-wire-protocol.md)  
User docs: [open-message/docs/user](../open-message/docs/user).

## Install

```bash
go get github.com/devotionalist/open-message-client-go@latest
```

## Usage

```go
import om "github.com/devotionalist/open-message-client-go"

c, err := om.Connect(ctx, om.Config{
  ServiceURL: "proxy.example:6650", // prod: Proxy; do not hard-code Broker
  AuthToken:  os.Getenv("OM_AUTH_TOKEN"),
})
p, err := c.CreateProducer(ctx, "tenant/ns/topic", "my-producer", true)
ledgerID, entryID, err := p.Send(ctx, []byte("payload"))

co, err := c.Subscribe(ctx, "tenant/ns/topic", "sub", "Exclusive")
_ = co.Flow(ctx, 10)
msg, err := co.Receive(ctx)
_ = co.AckCumulative(ctx, msg.LedgerID, int64(msg.EntryID))
```

## API (v0.1)

`Connect` · `Lookup` · `CreateProducer` / `Send` · `Subscribe` / `Flow` / `Receive` / `AckCumulative`

Delivery semantics: at-least-once; idempotent produce only within documented stages — see open-message user docs. **Do not claim exactly-once.**

## Develop

```bash
go test ./...
go run ./examples/produce-consume
```

## License

Apache-2.0
