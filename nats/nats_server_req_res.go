package nats

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
	msg, err := sv.instance.Request(subject, payload, timeout)
	if errors.Is(err, nats.ErrNoResponders) {
		log.Printf("[NATS SERVER]: request - no responders for subject: %s", subject)
	}
	return msg, err
}

func (sv *Nats) Publish(sub string, payload []byte) error {
	return sv.instance.Publish(sub, payload)
}

func (sv *Nats) PublishRequest(sub, reply string, data []byte) error {
	return sv.instance.PublishRequest(sub, reply, data)
}

// Reply ...
func (sv *Nats) Reply(msg *nats.Msg, payload []byte) error {
	return sv.instance.Publish(msg.Reply, payload)
}

// Response ...
func (sv *Nats) Response(msg *nats.Msg, payload interface{}, message string) error {
	res := NatsResponse{
		Success: false,
		Message: message,
		Data:    nil,
	}
	if message == "" {
		res.Success = true
		res.Data = ToBytes(payload)
	}
	err := sv.Reply(msg, ToBytes(res))
	if err != nil {
		fmt.Println("[ERROR] Response : ", err.Error())
	}
	return err
}

// Subscribe ...
func (sv *Nats) Subscribe(subject string, cb nats.MsgHandler) (*nats.Subscription, error) {
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
func (sv *Nats) QueueSubscribe(subject, queue string, cb nats.MsgHandler) (*nats.Subscription, error) {
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
