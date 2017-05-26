package producer

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

const mockedTopics = `["methode-articles","up-placeholders"]`

var producerConfigMock = MessageProducerConfig{
	Topic:         "methode-articles",
	Authorization: "my-first-auth-key",
}

func setupMockKafka(t *testing.T, status int, response string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if status != 200 {
			w.WriteHeader(status)
		} else {
			w.Write([]byte(response))
		}

		assert.Equal(t, "/topics", req.URL.Path)
		assert.Equal(t, "my-first-auth-key", req.Header.Get("Authorization"))
	}))
}

func TestHappyConnectivityCheck(t *testing.T) {
	proxy := setupMockKafka(t, 200, mockedTopics)
	defer proxy.Close()

	producerConfigMock.Addr = proxy.URL
	p := NewMessageProducer(producerConfigMock)
	msg, err := p.ConnectivityCheck()

	assert.Nil(t, err, "It should not return an error")
	assert.Equal(t, "Connectivity to producer proxy is OK.", msg, `The check message should be "Connectivity to producer proxy is OK."`)
}

func TestConnectivityCheckUnhappyKakfka(t *testing.T) {
	proxy := setupMockKafka(t, 500, "")
	defer proxy.Close()

	producerConfigMock.Addr = proxy.URL
	p := NewMessageProducer(producerConfigMock)
	msg, err := p.ConnectivityCheck()

	assert.EqualError(t, err, "Producer proxy returned status: 500", "It should return an error")
	assert.Equal(t, "Error connecting to producer proxy", msg, `The check message should be "Error connecting to producer proxy"`)
}

func TestConnectivityCheckNoKafkaProxy(t *testing.T) {
	producerConfigMock.Addr = "http://a-proxy-that-does-not-exist.com"
	p := NewMessageProducer(producerConfigMock)
	msg, err := p.ConnectivityCheck()

	assert.Error(t, err, "It should return an error")
	assert.Equal(t, "Error connecting to producer proxy", msg, `The check message should be "Error connecting to consumer proxy"`)
}
