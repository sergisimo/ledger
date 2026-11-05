package nats

import (
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// natsHeaderPrefix marks the headers the server and client reserve; they are
// not surfaced as message headers.
const natsHeaderPrefix = "Nats-"

// toNatsMsg maps a message to a NATS message on the topic subject. The ID
// travels as Nats-Msg-Id (enabling server-side deduplication) and the key in
// messaging.HeaderMessageKey.
func toNatsMsg(msg messaging.Message) *nats.Msg {
	out := &nats.Msg{Subject: msg.Topic, Data: msg.Payload, Header: nats.Header{}}
	for key, value := range msg.Headers {
		out.Header.Set(key, value)
	}
	if msg.ID != "" {
		out.Header.Set(jetstream.MsgIDHeader, msg.ID)
	}
	if msg.Key != "" {
		out.Header.Set(messaging.HeaderMessageKey, msg.Key)
	}

	return out
}

// toMessage maps a received JetStream message back to a message. One
// published outside this package without Nats-Msg-Id gets its stream
// coordinate as ID so consumers can still deduplicate redeliveries.
func toMessage(in jetstream.Msg) messaging.Message {
	msg := messaging.Message{Topic: in.Subject(), Payload: in.Data()}

	headers := in.Headers()
	msg.ID = headers.Get(jetstream.MsgIDHeader)
	msg.Key = headers.Get(messaging.HeaderMessageKey)

	msg.Headers = make(map[string]string, len(headers))
	for key := range headers {
		if key == messaging.HeaderMessageKey || strings.HasPrefix(key, natsHeaderPrefix) {
			continue
		}
		msg.Headers[key] = headers.Get(key)
	}

	if msg.ID == "" {
		if meta, err := in.Metadata(); err == nil {
			msg.ID = fmt.Sprintf("%s/%d", meta.Stream, meta.Sequence.Stream)
		}
	}

	return msg
}
