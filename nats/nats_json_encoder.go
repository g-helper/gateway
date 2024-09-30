package nats

import (
	"errors"
	"log"

	"github.com/nats-io/nats.go"
)

// JSONEncoder ...
type JSONEncoder struct {
	encConn *nats.EncodedConn
	config  Config
}

// Subscribe ...
func (e *JSONEncoder) Subscribe(subject string, cb nats.Handler) (*nats.Subscription, error) {
	sub, err := e.encConn.Subscribe(subject, cb)
	if err != nil {
		log.Printf("natsio.JSONEncoder.Subscribe err: %v\n", err)
	} else {
		log.Printf("natsio.JSONEncoder - subscribed to subject %s successfully\n", subject)
	}
	return sub, err
}

// QueueSubscribe ...
func (e *JSONEncoder) QueueSubscribe(subject string, queue string, cb nats.Handler) (*nats.Subscription, error) {
	sub, err := e.encConn.QueueSubscribe(subject, queue, cb)
	if err != nil {
		log.Printf("natsio.JSONEncoder.QueueSubscribe err: %v\n", err)
	} else {
		log.Printf("natsio.JSONEncoder.QueueSubscribe - subscribed to subject %s successfully\n", subject)
	}
	return sub, err
}

// Publish ...
func (e *JSONEncoder) Publish(reply string, data interface{}) error {
	return e.encConn.Publish(reply, data)
}

// Request ...
func (e *JSONEncoder) Request(subject string, data interface{}, res interface{}, isUseNameSpace bool) error {
	err := e.encConn.Request(subject, data, res, e.config.RequestTimeout)
	if errors.Is(err, nats.ErrNoResponders) {
		log.Printf("[NATS SERVER]: request - no responders for subject: %s", subject)
	}
	return err
}

func (e *JSONEncoder) RequestCommonNS(subject string, data interface{}, res interface{}) error {
	err := e.encConn.Request(subject, data, res, e.config.RequestTimeout)
	if errors.Is(err, nats.ErrNoResponders) {
		log.Printf("[NATS SERVER]: request - no responders for subject: %s", subject)
	}
	return err
}

type CommonResponse struct {
	Data  interface{} `json:"data"`
	Error string      `json:"error"`
}

func (e *JSONEncoder) response(reply string, data interface{}, err error) {
	res := CommonResponse{Data: data}
	if err != nil {
		res.Error = err.Error()
	}
	e.encConn.Publish(reply, res)
}
