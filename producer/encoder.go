package producer

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	//"github.com/Financial-Times/post-publication-combiner/v2/processor"
)

// Encoder encodes http body before sending it
type Encoder interface {
	// Encode map the bytes buffer to a different representation
	Encode(b []byte) (interface{}, error)
}

const (
	Base64E = iota
	CombinedModelE
)

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