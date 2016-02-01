package producer

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
)

const CONTENT_TYPE_HEADER = "application/vnd.kafka.binary.v1+json"

//Interface for message producer - which writes to kafka through the proxy
type MessageProducer interface {
	SendMessage(string, Message) error
}

type DefaultMessageProducer struct {
	config MessageProducerConfig
	client *http.Client
}

//Configuration for message producer
type MessageProducerConfig struct {
	//proxy address
	Addr  string `json:"address"`
	Topic string `json:"topic"`
	//the name of the queue
	//leave it empty for requests to UCS kafka-proxy
	Queue         string `json:"queue"`
	Authorization string `json:"authorization"`
}

//Message is the higher-level representation of messages from the queue: containing headers and message body
type Message struct {
	Headers map[string]string
	Body    string
}

//Message format required by Kafka-Proxy containing all the Messages
type MessageWithRecords struct {
	Records []MessageRecord `json:"records"`
}

//Message format required by Kafka-Proxy
type MessageRecord struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NewMessageProducer returns a producer instance
func NewMessageProducer(config MessageProducerConfig) MessageProducer {
	return &DefaultMessageProducer{config, &http.Client{}}
}

func (p *DefaultMessageProducer) SendMessage(uuid string, message Message) (err error) {

	//concatenate message headers with message body to form a proper message string to save
	messageString := buildMessage(message)
	return p.SendRawMessage(uuid, messageString)
}

func (p *DefaultMessageProducer) SendRawMessage(uuid string, message string) (err error) {

	//encode in base64 and envelope the message
	envelopedMessage, err := envelopeMessage(uuid, message)
	if err != nil {
		return
	}

	//create request
	req, err := constructRequest(p.config.Addr, p.config.Topic, p.config.Queue, p.config.Authorization, envelopedMessage)

	//make request
	resp, err := p.client.Do(req)
	if err != nil {
		log.Printf("ERROR - executing request: %s", err.Error())
		return
	}
	defer resp.Body.Close()

	//send - verify response status
	//log if error happens
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("Unexpected response status %d. Expected: %d.", resp.StatusCode, http.StatusOK)
		log.Printf("ERROR - %s %s", err.Error(), resp.Request.URL.String())
		return err
	}

	return nil
}

func constructRequest(addr string, topic string, queue string, authorizationKey string, message string) (*http.Request, error) {

	req, err := http.NewRequest("POST", addr+"/topics/"+topic, strings.NewReader(message))
	if err != nil {
		log.Printf("ERROR - creating request: %s", err.Error())
		return req, err
	}

	//set content-type header to json, and host header according to vulcand routing strategy
	req.Header.Add("Content-Type", CONTENT_TYPE_HEADER)
	if len(queue) > 0 {
		req.Host = queue
	}

	if len(authorizationKey) > 0 {
		req.Header.Add("Authorization", authorizationKey)
	}

	return req, err
}

func buildMessage(message Message) string {

	builtMessage := "FTMSG/1.0\n"

	var keys []string

	//order headers
	for header := range message.Headers {
		keys = append(keys, header)
	}
	sort.Strings(keys)

	//set headers
	for _, key := range keys {
		builtMessage = builtMessage + key + ": " + message.Headers[key] + "\n"
	}

	builtMessage = builtMessage + "\n" + message.Body

	return builtMessage

}

func envelopeMessage(key string, message string) (string, error) {

	if key == "" {
		log.Printf("Key cannot be empty. Please provide a valid UUID!")
		return "", errors.New("Key cannot be empty. Please provide a valid UUID!")
	}

	key64 := base64.StdEncoding.EncodeToString([]byte(key))
	message64 := base64.StdEncoding.EncodeToString([]byte(message))

	record := MessageRecord{Key: key64, Value: message64}
	msgWithRecords := &MessageWithRecords{Records: []MessageRecord{record}}

	jsonRecords, err := json.Marshal(msgWithRecords)

	if err != nil {
		log.Printf("ERROR - marshalling in json: %s", err.Error())
		return "", err
	}

	return string(jsonRecords), err
}
