package producer

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeBase64(t *testing.T) {
	// Giving
	var encoder = NewEncoder(Base64E)
	msg, err := uuid.NewUUID()
	assert.NoError(t, err, "unable to generate uuid as test input")
	b, err := msg.MarshalBinary()
	assert.NoError(t, err, "unable to serialize uuid test input")

	// When
	v, err := encoder.Encode(b)

	// Then
	assert.NoError(t, err, "unable to encode data")
	str, ok := v.(string)
	assert.True(t, ok)
	assert.NotEmpty(t, str, "empty string returned")
	assert.Equal(t, base64.StdEncoding.EncodeToString(b), str)
}

func TestEncodeModel(t *testing.T) {
	// Giving
	var encoder = NewEncoder(CombinedModelE)
	v, err := generateRandomCombinedModel()
	assert.NoError(t, err, "unable to generate random combined model")
	var out bytes.Buffer
	jsonEncoder := json.NewEncoder(&out)
	jsonEncoder.SetIndent("", "\t")
	assert.NoError(t, jsonEncoder.Encode(v), "unable to serialize random combined model in json")
	b := out.Bytes()

	// When
	encoded, err := encoder.Encode(b)

	// Then
	assert.NoError(t, err, "unable to encode data")
	cm, ok := encoded.(CombinedModel)
	assert.True(t, ok)
	out.Reset()
	enc2 := json.NewEncoder(&out)
	enc2.SetIndent("", "\t")
	assert.NoError(t, enc2.Encode(cm), "unable to serialize random combined model in json")
	b2 := out.Bytes()
	assert.Equal(t, b, b2, "results before/after decoding mismatch")
}

func TestEnvelopeMessage2(t *testing.T) {
	var tests = []struct {
		key              string
		message          string
		envelopedMessage string
		err              error
	}{
		{
			"3d91a94c-6ce6-4ec9-a16b-8b89be574ecc",
			"a simple test message",
			"{\"records\":[{\"key\":\"M2Q5MWE5NGMtNmNlNi00ZWM5LWExNmItOGI4OWJlNTc0ZWNj\",\"value\":\"YSBzaW1wbGUgdGVzdCBtZXNzYWdl\"}]}",
			nil,
		},
		{
			"",
			"a simple test message",
			"{\"records\":[{\"key\":\"\",\"value\":\"YSBzaW1wbGUgdGVzdCBtZXNzYWdl\"}]}",
			nil,
		},
		{
			"3d91a94c-6ce6-4ec9-a16b-8b89be574ecc",
			"",
			"{\"records\":[{\"key\":\"M2Q5MWE5NGMtNmNlNi00ZWM5LWExNmItOGI4OWJlNTc0ZWNj\",\"value\":\"\"}]}",
			nil,
		},
	}

	for _, test := range tests {
		resultingMessage, err := envelopeMessage(test.key, test.message, NewEncoder(Base64E))
		if resultingMessage != test.envelopedMessage || (err != test.err && err.Error() != test.err.Error()) {
			t.Errorf("Expected: msgs: %v, error: %v\nActual: msgs: %v, error: %v.",
				test.envelopedMessage, test.err, resultingMessage, err)
		}
	}
}