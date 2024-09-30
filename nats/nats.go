package nats

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/thoas/go-funk"
)

// Nats ...
type Nats struct {
	instance *nats.Conn
	Config   Config
}

// JetStream ...
type JetStream struct {
	instance nats.JetStreamContext
}

// Config ...
type Config struct {
	// Connect url
	URL string

	// Auth user
	User string

	// Auth password
	Password string

	// TLS config
	TLS *TLSConfig

	// RequestTimeout
	RequestTimeout time.Duration

	Debug bool
}

// TLSConfig ...
type TLSConfig struct {
	// Cert file
	CertFilePath string

	// Key file
	KeyFilePath string

	// Root CA
	RootCAFilePath string
}

var (
	natsServer    *Nats
	natsJetStream JetStream
)

// Connect ...
func Connect(cfg Config) error {
	if cfg.URL == "" {
		return errors.New("natsio: connect URL is required")
	}

	// Connect options
	opts := []nats.Option{
		nats.ReconnectWait(2 * time.Second), // Time to wait before attempting reconnection
		nats.MaxReconnects(-1),              // Unlimited reconnections
	}

	// Has authentication
	if cfg.User != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}

	// If it has TLS
	if cfg.TLS != nil {
		opts = append(opts, nats.ClientCert(cfg.TLS.CertFilePath, cfg.TLS.KeyFilePath))
		opts = append(opts, nats.RootCAs(cfg.TLS.RootCAFilePath))
	}

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		msg := fmt.Sprintf("natsio: error when connecting to NATS: %s", err.Error())
		return errors.New(msg)
	}

	fmt.Printf("⚡️[natsio]: connected to %s \n", cfg.URL)

	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = requestTimeout
	}

	// Set client
	natsServer = &Nats{
		instance: nc,
		Config:   cfg,
	}

	// Create jet stream context
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		msg := fmt.Sprintf("natsio: error when create NATS JetStream: %s", err.Error())
		return errors.New(msg)
	}
	natsJetStream.instance = js

	return nil
}

// GetServer ...
func GetServer() *Nats {
	return natsServer
}

// GetJetStream ...
func GetJetStream() JetStream {
	return natsJetStream
}

// mergeAndUniqueArrayStrings ...
func mergeAndUniqueArrayStrings(arr1, arr2 []string) []string {
	var result = make([]string, 0)
	result = append(result, arr1...)
	result = append(result, arr2...)
	result = funk.UniqString(result)
	return result
}

// generateSubjectNames ...
func generateSubjectNames(streamName string, subjects []string) []string {
	var result = make([]string, 0)
	for _, subject := range subjects {
		name := combineStreamAndSubjectName(streamName, subject)
		result = append(result, name)
	}
	return result
}

func combineStreamAndSubjectName(stream, subject string) string {
	return fmt.Sprintf("%s.%s", stream, subject)
}

func generateStreamConfig(stream string, subjects []string) *nats.StreamConfig {
	cfg := nats.StreamConfig{
		Name:         stream,
		Subjects:     subjects,
		Retention:    nats.WorkQueuePolicy,
		MaxConsumers: -1,
		MaxMsgSize:   -1,
		MaxMsgs:      -1,
		NoAck:        false,
	}
	return &cfg
}

// GetStreamInfo ...
func (js JetStream) GetStreamInfo(name string) (*nats.StreamInfo, error) {
	return js.instance.StreamInfo(name)
}

// AddStream add new stream, with default config
// Due to subject must have a unique name, subject name will be combined with stream name
// E.g: stream name is "DEMO", subject name is "Subject-1", so final name in NATS will be: DEMO.Subject-1
func (js JetStream) AddStream(name string, subjects []string) error {
	// Get info about the stream
	stream, _ := js.GetStreamInfo(name)

	// If stream not found, create new
	if stream == nil {
		subjectNames := generateSubjectNames(name, subjects)
		_, err := js.instance.AddStream(generateStreamConfig(name, subjectNames))
		if err != nil {
			msg := fmt.Sprintf("[NATS JETSTREAM] - add stream error: %s", err.Error())
			return errors.New(msg)
		}
	}

	return nil
}

// DeleteStream ...
func (js JetStream) DeleteStream(name string) error {
	if err := js.instance.DeleteStream(name); err != nil {
		msg := fmt.Sprintf("[NATS JETSTREAM] - delete stream error: %s", err.Error())
		return errors.New(msg)
	}
	return nil
}

// AddStreamSubjects ...
func (js JetStream) AddStreamSubjects(name string, subjects []string) error {
	// Get info about the stream
	stream, _ := js.GetStreamInfo(name)
	if stream == nil {
		msg := fmt.Sprintf("[NATS JETSTREAM] - error when adding stream %s subjects: stream not found", name)
		return errors.New(msg)
	}

	// Merge current and new subjects
	subjectNames := generateSubjectNames(name, subjects)
	newSubjects := mergeAndUniqueArrayStrings(subjectNames, stream.Config.Subjects)

	_, err := js.instance.UpdateStream(generateStreamConfig(name, newSubjects))
	if err != nil {
		msg := fmt.Sprintf("[NATS JETSTREAM] - add stream error: %s", err.Error())
		return errors.New(msg)
	}
	return nil
}

// GetConsumerInfo ...
func (js JetStream) GetConsumerInfo(stream, name string) (*nats.ConsumerInfo, error) {
	return js.instance.ConsumerInfo(stream, name)
}

// AddConsumer ...
func (js JetStream) AddConsumer(stream, subject, name string) error {
	// Get consumer first, return if existed
	consumer, err := js.GetConsumerInfo(stream, name)
	if consumer != nil {
		return nil
	}

	// Generate channel name
	channel := combineStreamAndSubjectName(stream, subject)

	// Add
	_, err = js.instance.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       name,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: channel,
	})

	if err != nil {
		msg := fmt.Sprintf("[NATS JETSTREAM] - add consumer %s for stream #%s error: %s", name, stream, err.Error())
		return errors.New(msg)
	}
	return nil
}

// Publish ...
func (js JetStream) Publish(stream, subject string, payload []byte) error {
	channel := combineStreamAndSubjectName(stream, subject)

	_, err := js.instance.PublishAsync(channel, payload)
	if err != nil {
		msg := fmt.Sprintf("[NATS JETSTREAM] - publish message to subject #%s error: %s", channel, err.Error())
		return errors.New(msg)
	}
	return nil
}

// Subscribe ...
func (js JetStream) Subscribe(stream, subject string, cb nats.MsgHandler) (*nats.Subscription, error) {
	channel := combineStreamAndSubjectName(stream, subject)

	sub, err := js.instance.Subscribe(channel, cb)
	if err != nil {
		msg := fmt.Sprintf("[NATS JETSTREAM] - subscribe subject %s error: %s", channel, err.Error())
		return nil, errors.New(msg)
	}
	return sub, nil
}

// PullSubscribe ...
func (js JetStream) PullSubscribe(stream, subject, consumer string) (*nats.Subscription, error) {
	channel := combineStreamAndSubjectName(stream, subject)

	// Check if consumer existed
	con, err := js.GetConsumerInfo(stream, consumer)
	if con == nil || err != nil {
		msg := fmt.Sprintf("[NATS JETSTREAM] - pull subscribe consumer %s not existed in stream %s", consumer, stream)
		return nil, errors.New(msg)
	}

	// Pull
	sub, err := js.instance.PullSubscribe(channel, consumer)
	if err != nil {
		msg := fmt.Sprintf("[NATS JETSTREAM] - pull subscribe subject #%s - consumer #%s error: %s", channel, consumer, err.Error())
		return nil, errors.New(msg)
	}

	return sub, nil
}

// QueueSubscribe ...
func (js JetStream) QueueSubscribe(stream, subject, queueName string, cb nats.MsgHandler) error {
	channel := combineStreamAndSubjectName(stream, subject)

	_, err := js.instance.QueueSubscribe(channel, queueName, cb)
	if err != nil {
		msg := fmt.Sprintf("[NATS JETSTREAM] - queue subscribe with subject #%s error: %s", channel, err.Error())
		return errors.New(msg)
	}
	return nil
}

// CLIENT ...

// ClientRequest ...
func ClientRequest[REQUEST any, RESPONSE any](subject string, req REQUEST) (RESPONSE, error) {
	var (
		traceId = uuid.New().String()
	)
	if GetServer().Config.Debug {
		fmt.Println(fmt.Sprintf("[%s] [REQUEST] [%s] with data %v", traceId, subject, req))
	}
	var (
		res     RESPONSE
		natsRes NatsResponse
	)
	msg, err := GetServer().Request(subject, ToBytes(req))
	if err != nil {
		return res, err
	}
	if GetServer().Config.Debug {
		fmt.Println(fmt.Sprintf("[%s] [RECEIVED] [%s] with data %v", traceId, subject, string(msg.Data)))
	}
	if err = json.Unmarshal(msg.Data, &natsRes); err != nil {
		return res, err
	}

	if err = json.Unmarshal(natsRes.Data, &res); err != nil {
		return res, err
	}
	if GetServer().Config.Debug {
		fmt.Println(fmt.Sprintf("[%s] [RESPONSE] [%s] with data %v", traceId, subject, string(msg.Data)))
	}
	return res, nil
}
