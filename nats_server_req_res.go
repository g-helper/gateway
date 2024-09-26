package gateway

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// Default timeout 10s
const requestTimeout = 10 * time.Second

// Request ...
func (sv *Nats) Request(subject string, payload []byte) (*nats.Msg, error) {
	timeout := requestTimeout
	if sv.Config.RequestTimeout > 0 {
		timeout = sv.Config.RequestTimeout
	}
	subject = sv.Config.Namespace + subject
	msg, err := sv.instance.Request(subject, payload, timeout)
	if errors.Is(err, nats.ErrNoResponders) {
		log.Printf("[NATS SERVER]: request - no responders for subject: %s", subject)
	}
	return msg, err
}

// RequestWithNS ...
func (sv *Nats) RequestWithNS(subject string, withNS bool, payload []byte) (*nats.Msg, error) {
	if withNS {
		subject = sv.Config.Namespace + subject
	}
	timeout := requestTimeout
	if sv.Config.RequestTimeout > 0 {
		timeout = sv.Config.RequestTimeout
	}
	msg, err := sv.instance.Request(subject, payload, timeout)
	if errors.Is(err, nats.ErrNoResponders) {
		log.Printf("[NATS SERVER]: request - no responders for subject: %s", subject)
	}
	return msg, err
}

func (sv *Nats) PublishWithNameSpace(sub string, nameSpace string, payload []byte) error {
	sub = nameSpace + sub
	fmt.Println("Publish to -> ", sub)
	return sv.instance.Publish(sub, payload)
}

func (sv *Nats) Publish(sub string, withNS bool, payload []byte) error {
	if withNS {
		sub = sv.Config.Namespace + sub
	}
	return sv.instance.Publish(sub, payload)
}

func (sv *Nats) PublishRequest(sub, reply string, withNS bool, data []byte) error {
	if withNS {
		sub = sv.Config.Namespace + sub
	}
	return sv.instance.PublishRequest(sub, reply, data)
}

// Reply ...
func (sv *Nats) Reply(msg *nats.Msg, payload []byte) error {
	return sv.instance.Publish(msg.Reply, payload)
}

// Subscribe ...
func (sv *Nats) Subscribe(subject string, withNS bool, cb nats.MsgHandler) (*nats.Subscription, error) {
	if withNS {
		subject = sv.Config.Namespace + subject
	}
	sub, err := sv.instance.Subscribe(subject, cb)
	if err != nil {
		msg := fmt.Sprintf("[NATS SERVER] - subscribe subject %s error: %s", subject, err.Error())
		return nil, errors.New(msg)
	}
	return sub, nil
}

func (sv *Nats) SubscribeSync() (*nats.Subscription, error) {
	return sv.instance.SubscribeSync(nats.NewInbox())
}

// QueueSubscribe ...
func (sv *Nats) QueueSubscribe(subject, queue string, withNS bool, cb nats.MsgHandler) (*nats.Subscription, error) {
	if withNS {
		subject = sv.Config.Namespace + subject
	}
	sub, err := sv.instance.QueueSubscribe(subject, queue, cb)
	if err != nil {
		msg := fmt.Sprintf("[NATS SERVER] - queue subscribe subject %s, error: %s", subject, err.Error())
		return nil, errors.New(msg)
	}
	log.Println("[NATS SERVER] - queue subscribe subject: ", subject)
	return sub, nil
}

// NewJSONEncodedConn ...
func (sv *Nats) NewJSONEncodedConn() (*JSONEncoder, error) {
	enc, err := nats.NewEncodedConn(sv.instance, nats.JSON_ENCODER)
	if err != nil {
		log.Printf("natsio.NewJSONEncodedConn: err %v\n", err)
		return nil, err
	}
	return &JSONEncoder{
		encConn: enc,
		config:  sv.Config,
	}, nil
}

func (sv *Nats) SetNamespace(ns string) error {
	if ns == "" {
		return fmt.Errorf("nats: namespace can not empty")
	}
	sv.Config.Namespace = ns
	return nil
}
