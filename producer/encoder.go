package producer

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	//"github.com/Financial-Times/post-publication-combiner/v2/processor"
)

const crlf = "\r\n"

const (
	Base64E = iota
	CombinedModelE
)

// Encoder encodes http body before sending it
type Encoder interface {
	// Encode map the b buffer to a different representation
	Encode(b []byte) (interface{}, error)

	// EncodeKey map the k buffer to a string
	EncodeKey(k []byte) (string, error)

	// ContentType return an appropriate http content type header for this encoding
	// FIXME this should be improved based on the producer version too, not only the encoding
	ContentType() string

	// BuildMessage build the http body
   BuildMessage(message Message) string
}

func NewEncoder(t int) Encoder {
	switch t {
	case Base64E:
		return &Base64Encoder{}
	case CombinedModelE:
		return &CombinedModelEncoder{}
	default:
		return &Base64Encoder{}
	}
}

// Base64Encoder encodes to base64
type Base64Encoder struct {}

// Encode encode the buffer in base 64
func (encoder Base64Encoder) Encode(b []byte) (interface{}, error) {
	return base64.StdEncoding.EncodeToString(b), nil
}

// EncodeKey encode the buffer in base 64
func (encoder Base64Encoder) EncodeKey(k []byte) (string, error) {
	v, err := encoder.Encode(k)
	return v.(string), err
}

// ContentType binary for base 64
func (encoder Base64Encoder) ContentType() string {
	return "application/vnd.kafka.binary.v1+json"
}

// BuildMessage create a ft format with the headers embedded
// concatenate message headers with message body to form a proper message string to save
func (encoder Base64Encoder) BuildMessage(message Message) string {
	builtMessage := "FTMSG/1.0" + crlf
	var keys []string
	//order headers
	for header := range message.Headers {
		keys = append(keys, header)
	}
	sort.Strings(keys)
	//set headers
	for _, key := range keys {
		builtMessage = builtMessage + key + ": " + message.Headers[key] + crlf
	}
	builtMessage = builtMessage + crlf + message.Body
	return builtMessage
}

// CombinedModelEncoder encodes to struct
type CombinedModelEncoder struct {}

// Encode map the bytes buffer to CombinedModel data structure
func (encoder CombinedModelEncoder) Encode(b []byte) (interface{}, error) {
	var data CombinedModel
	//var data processor.CombinedModel
	if err := json.Unmarshal(b, &data); err != nil {
		errMsg := fmt.Sprintf("ERROR - unmarshalling in json: %s", err.Error())
		return nil, errors.New(errMsg)
	}
	return data, nil
}

// EncodeKey encode the buffer in base 64
func (encoder CombinedModelEncoder) EncodeKey(k []byte) (string, error) {
	return string(k), nil
}

// ContentType json for model mapping
func (encoder CombinedModelEncoder) ContentType() string {
	return "application/vnd.kafka.json.v2+json"
}

// BuildMessage get a plain json body
// FIXME This implementation is ignoring the message.Headers
func (encoder CombinedModelEncoder) BuildMessage(message Message) string {
	return message.Body
}