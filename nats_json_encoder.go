package gateway

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
func (e *JSONEncoder) Subscribe(subject string, cb nats.Handler, isUseNameSpace bool) (*nats.Subscription, error) {
	if isUseNameSpace {
		subject = e.getSubject(subject)
	}
	sub, err := e.encConn.Subscribe(subject, cb)
	if err != nil {
		log.Printf("natsio.JSONEncoder.Subscribe err: %v\n", err)
	} else {
		log.Printf("natsio.JSONEncoder - subscribed to subject %s successfully\n", subject)
	}
	return sub, err
}

// QueueSubscribe ...
func (e *JSONEncoder) QueueSubscribe(subject string, cb nats.Handler, isUseNameSpace bool) (*nats.Subscription, error) {
	if isUseNameSpace {
		subject = e.getSubject(subject)
	}
	sub, err := e.encConn.QueueSubscribe(subject, subject, cb)
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

// RequestWithSpecNameSpace ...
func (e *JSONEncoder) RequestWithSpecNameSpace(subject string, data interface{}, res interface{}, ns string) error {
	if ns != "" {
		subject = ns + ":" + subject
	}
	err := e.encConn.Request(subject, data, res, e.config.RequestTimeout)
	if errors.Is(err, nats.ErrNoResponders) {
		log.Printf("[NATS SERVER]: request - no responders for subject: %s", subject)
	}
	return err
}

// Request ...
func (e *JSONEncoder) Request(subject string, data interface{}, res interface{}, isUseNameSpace bool) error {
	if isUseNameSpace {
		subject = e.getSubject(subject)
	}
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

func (e *JSONEncoder) getSubject(sub string) string {
	if e.config.Namespace == "*" {
		return sub
	}
	return e.config.Namespace + ":" + sub
}
