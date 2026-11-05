package rabbitmq

import (
	"fmt"

	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// toAmqpMessage maps a message to an AMQP 1.0 message addressed to the
// topic's exchange: the ID becomes the message ID, the key the subject
// (routing key) and the headers the application properties.
func toAmqpMessage(msg messaging.Message) (*amqp.Message, error) {
	out, err := rabbitmqamqp.NewMessageWithAddress(
		msg.Payload,
		&rabbitmqamqp.ExchangeAddress{
			Exchange: msg.Topic,
			Key:      msg.Key,
		},
	)
	if err != nil {
		return nil, err
	}

	out.Properties.MessageID = msg.ID
	if msg.Key != "" {
		out.Properties.Subject = &msg.Key
	}

	if len(msg.Headers) > 0 {
		out.ApplicationProperties = make(map[string]any, len(msg.Headers))
		for key, value := range msg.Headers {
			out.ApplicationProperties[key] = value
		}
	}

	return out, nil
}

// toMessage maps a received AMQP 1.0 message back to a message on topic. A
// message published outside this package may carry a non-string or no
// message ID; it is stringified or left empty.
func toMessage(topic string, in *amqp.Message) messaging.Message {
	msg := messaging.Message{Topic: topic, Payload: in.GetData()}

	if in.Properties != nil {
		if in.Properties.MessageID != nil {
			if id, ok := in.Properties.MessageID.(string); ok {
				msg.ID = id
			} else {
				msg.ID = fmt.Sprint(in.Properties.MessageID)
			}
		}
		if in.Properties.Subject != nil {
			msg.Key = *in.Properties.Subject
		}
	}

	if len(in.ApplicationProperties) > 0 {
		msg.Headers = make(map[string]string, len(in.ApplicationProperties))
		for key, value := range in.ApplicationProperties {
			if s, ok := value.(string); ok {
				msg.Headers[key] = s
			} else {
				msg.Headers[key] = fmt.Sprint(value)
			}
		}
	}

	return msg
}
