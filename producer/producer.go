package producer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

// MessageProducer defines the interface for message producer - which writes
// to kafka through the proxy
//
// SendMessage implements the logic to sending a Message to a queue.
// The input string should be the UUID that identifies the message.
// An error should be returned in case of failure in sending a message.
//
// ConnectivityCheck implements the logic to check the current
// connectivity to the queue.
// The method should return a message about the status of the connection and
// an error in case of connectivity failure.
type MessageProducer interface {
	SendMessage(string, Message) error
	ConnectivityCheck() (string, error)
}

// DefaultMessageProducer defines default implementation of a message producer
type DefaultMessageProducer struct {
	config MessageProducerConfig
	client *http.Client
	encoder Encoder
}

// MessageProducerConfig specifies the configuration for message producer
type MessageProducerConfig struct {
	//proxy address
	Addr  string `json:"address"`
	Topic string `json:"topic"`
	//the name of the queue
	//leave it empty for requests to UCS kafka-proxy
	Queue         string `json:"queue"`
	Authorization string `json:"authorization"`
}

// Message is the higher-level representation of messages from the queue: containing headers and message body
type Message struct {
	Headers map[string]string
	Body    string
}

// MessageWithRecords is a message format required by Kafka-Proxy containing all the Messages
type MessageWithRecords struct {
	Records []MessageRecord `json:"records"`
}

// MessageRecord is a Message format required by Kafka-Proxy
type MessageRecord struct {
	Key   string `json:"key"`
	Value interface{} `json:"value"`
}

// NewMessageProducerWithEncoder returns a producer instance with an specific encoder
func NewMessageProducerWithEncoder(config MessageProducerConfig, encoder Encoder) MessageProducer {
	return NewMessageProducerWithHTTPClient(config, &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 100,
		}}, encoder)
}

// NewMessageProducer returns a producer instance
func NewMessageProducer(config MessageProducerConfig) MessageProducer {
	return NewMessageProducerWithHTTPClient(config, &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 100,
		}}, NewEncoder(Base64E))
}

// NewMessageProducerWithHTTPClient returns a producer instance with specified http client instance
func NewMessageProducerWithHTTPClient(config MessageProducerConfig, httpClient *http.Client, encoder Encoder) MessageProducer {
	return &DefaultMessageProducer{config, httpClient, encoder}
}

// SendMessage is the producer method that takes care of sending a message on the queue
func (p *DefaultMessageProducer) SendMessage(uuid string, message Message) (err error) {
	messageString := p.encoder.BuildMessage(message)
	return p.SendRawMessage(uuid, messageString, p.encoder.ContentType())
}

// SendRawMessage is the producer method that takes care of sending a raw message on the queue
func (p *DefaultMessageProducer) SendRawMessage(uuid string, message, contentTypeHeader string) (err error) {

	//encode in base64 and envelope the message
	envelopedMessage, err := envelopeMessage(uuid, message, p.encoder)
	if err != nil {
		return
	}

	//create request
	req, err := constructRequest(p.config.Addr, p.config.Topic, p.config.Queue, p.config.Authorization, envelopedMessage, contentTypeHeader)

	//make request
	resp, err := p.client.Do(req)
	if err != nil {
		errMsg := fmt.Sprintf("ERROR - executing request: %s", err.Error())
		return errors.New(errMsg)
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	//send - verify response status
	//log if error happens
	if resp.StatusCode != http.StatusOK {
		errMsg := fmt.Sprintf("ERROR - Unexpected response status %d. Expected: %d. %s", resp.StatusCode, http.StatusOK, resp.Request.URL.String())
		return errors.New(errMsg)
	}

	return nil
}

func constructRequest(addr string, topic string, queue string, authorizationKey string, message, contentTypeHeader string) (*http.Request, error) {

	req, err := http.NewRequest("POST", addr+"/topics/"+topic, strings.NewReader(message))
	if err != nil {
		errMsg := fmt.Sprintf("ERROR - creating request: %s", err.Error())
		return req, errors.New(errMsg)
	}

	//set content-type header to json, and host header according to vulcand routing strategy
	req.Header.Add("Content-Type", contentTypeHeader)
	if len(queue) > 0 {
		req.Host = queue
	}

	if len(authorizationKey) > 0 {
		req.Header.Add("Authorization", authorizationKey)
	}

	return req, err
}

func envelopeMessage(key string, message string, encoder Encoder) (string, error) {

	if key != "" {
		var err error
		if key, err = encoder.EncodeKey([]byte(key)); err != nil {
			return "", err
		}
	}

	var data interface{}
	var err error
	if data, err = encoder.Encode([]byte(message)); err != nil {
		return "", err
	}
	record := MessageRecord{Key: key, Value: data}
	msgWithRecords := &MessageWithRecords{Records: []MessageRecord{record}}

	jsonRecords, err := json.Marshal(msgWithRecords)

	if err != nil {
		errMsg := fmt.Sprintf("ERROR - marshalling in json: %s", err.Error())
		return "", errors.New(errMsg)
	}

	return string(jsonRecords), err
}

// ConnectivityCheck verifies if the kafka proxy is available
func (p *DefaultMessageProducer) ConnectivityCheck() (string, error) {
	err := p.checkMessageQueueProxyReachable()
	if err == nil {
		return "Connectivity to producer proxy is OK.", nil
	}
	log.Printf("ERROR - Producer Connectivity Check - %s", err)
	return "Error connecting to producer proxy", err
}

func (p *DefaultMessageProducer) checkMessageQueueProxyReachable() error {
	req, err := http.NewRequest("GET", p.config.Addr+"/topics", nil)
	if err != nil {
		return fmt.Errorf("Could not connect to proxy: %v", err.Error())
	}

	if len(p.config.Authorization) > 0 {
		req.Header.Add("Authorization", p.config.Authorization)
	}

	if len(p.config.Queue) > 0 {
		req.Host = p.config.Queue
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("Could not connect to proxy: %v", err.Error())
	}
	defer cleanUp(resp)

	if resp.StatusCode != http.StatusOK {
		errMsg := fmt.Sprintf("Producer proxy returned status: %d", resp.StatusCode)
		return errors.New(errMsg)
	}
	return nil
}

func cleanUp(resp *http.Response) {
	_, err := io.Copy(ioutil.Discard, resp.Body)
	if err != nil {
		log.Printf("WARN - %s", err.Error())
	}

	err = resp.Body.Close()
	if err != nil {
		log.Printf("WARN - %s", err.Error())
	}
}
