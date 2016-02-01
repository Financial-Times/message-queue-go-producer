package producer

import (
	"errors"
	"github.com/golang/go/src/pkg/io/ioutil"
	"io"
	"testing"
)

func TestBuildMessage(t *testing.T) {
	var tests = []struct {
		message      Message
		builtMessage string
	}{
		{
			Message{
				map[string]string{
					"Content-Type":      "application/json",
					"Message-Id":        "c6653374-922c-4b78-927d-15c5125fcd8d",
					"Message-Timestamp": "2015-10-21T14:22:06.270Z",
					"Message-Type":      "cms-content-published",
					"Origin-System-Id":  "http://cmdb.ft.com/systems/methode-web-pub",
					"X-Request-Id":      "SYNTHETIC-REQ-MON_A391MMaVMv",
				},
				`{"contentUri":"http://methode-image-model-transformer-pr-uk-int.svc.ft.com/image/model/c94a3a57-3c99-423c-a6bd-ed8c4c10a3c3",
"uuid":"c94a3a57-3c99-423c-a6bd-ed8c4c10a3c3", "destination":"methode-image-model-transformer", "relativeUrl":"/image/model/c94a3a57-3c99-423c-a6bd-ed8c4c10a3c3"}`,
			},
			`FTMSG/1.0
Content-Type: application/json
Message-Id: c6653374-922c-4b78-927d-15c5125fcd8d
Message-Timestamp: 2015-10-21T14:22:06.270Z
Message-Type: cms-content-published
Origin-System-Id: http://cmdb.ft.com/systems/methode-web-pub
X-Request-Id: SYNTHETIC-REQ-MON_A391MMaVMv

{"contentUri":"http://methode-image-model-transformer-pr-uk-int.svc.ft.com/image/model/c94a3a57-3c99-423c-a6bd-ed8c4c10a3c3",
"uuid":"c94a3a57-3c99-423c-a6bd-ed8c4c10a3c3", "destination":"methode-image-model-transformer", "relativeUrl":"/image/model/c94a3a57-3c99-423c-a6bd-ed8c4c10a3c3"}`,
		},
	}

	for _, test := range tests {
		resultingMessage := buildMessage(test.message)
		if resultingMessage != test.builtMessage {
			t.Errorf("Expected: msgs: %v\nActual: msgs: %v.",
				test.builtMessage, resultingMessage)
		}
	}
}

func TestEnvelopeMessage(t *testing.T) {
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
			"",
			errors.New("Key cannot be empty. Please provide a valid UUID!"),
		},
		{
			"3d91a94c-6ce6-4ec9-a16b-8b89be574ecc",
			"",
			"{\"records\":[{\"key\":\"M2Q5MWE5NGMtNmNlNi00ZWM5LWExNmItOGI4OWJlNTc0ZWNj\",\"value\":\"\"}]}",
			nil,
		},
	}

	for _, test := range tests {
		resultingMessage, err := envelopeMessage(test.key, test.message)
		if resultingMessage != test.envelopedMessage || (err != test.err && err.Error() != test.err.Error()) {
			t.Errorf("Expected: msgs: %v, error: %v\nActual: msgs: %v, error: %v.",
				test.envelopedMessage, test.err, resultingMessage, err)
		}
	}
}

func TestConstructRequest(t *testing.T) {
	var tests = []struct {
		addr               string
		topic              string
		queue              string
		authorizationKey   string
		message            string
		expectedRequestURL string
		expectedError      error
	}{
		{
			"https://localhost:8080",
			"test",
			"kafka-proxy",
			"Basic Y29jbzpjMGMwcGxhjhjhkhm0=",
			"simple message",
			"https://localhost:8080/topics/test",
			nil,
		},
		{
			"https://localhost:8080",
			"test",
			"kafka-proxy",
			"",
			`FTMSG/1.0
			Message-Id: c4b96810-03e8-4057-84c5-dcc3a8c61a26
			Message-Timestamp: 2015-10-19T09:30:29.110Z
			Message-Type: cms-content-published
			Origin-System-Id: http://cmdb.ft.com/systems/methode-web-pub
			Content-Type: application/json
			X-Request-Id: SYNTHETIC-REQ-MON_Unv1K838lY

			{"uuid":"e7a3b814-59ee-459e-8f60-517f3e80ed99", "value":"test","attributes":[]}`,
			"https://localhost:8080/topics/test",
			nil,
		},
	}

	for _, test := range tests {
		request, err := constructRequest(test.addr, test.topic, test.queue, test.authorizationKey, test.message)
		if request.URL.String() != test.expectedRequestURL {
			t.Errorf("Expected: url: %v, \nActual: url: %v.",
				test.expectedRequestURL, request.URL)
		} else if request.Host != test.queue && len(test.queue) > 0 {
			t.Errorf("Expected: host: %v, \nActual: host: %v.",
				test.queue, request.Host)
		} else if request.Header.Get("Authorization") != test.authorizationKey {
			t.Errorf("Expected: authorization: %v, \nActual: authorization: %v.",
				test.authorizationKey, request.Header.Get("Authorization"))
		} else if !containsMessage(request.Body, test.message) {
			t.Errorf("Expected: message: %v, \nActual: message: %v.",
				test.expectedRequestURL, request.URL)
		} else if request.Header.Get("Content-Type") != CONTENT_TYPE_HEADER {
			t.Errorf("Expected: Content-Type: %v\nActual: Content-Type: %v.",
				CONTENT_TYPE_HEADER, request.Header.Get("Content-Type"))
		} else if err != test.expectedError && err.Error() != test.expectedError.Error() {
			t.Errorf("Expected: error: %v\nActual: error: %v.",
				test.expectedError, err)
		}
	}
}

func containsMessage(body io.ReadCloser, message string) bool {
	if body == nil {
		return false
	}
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return false
	} else if string(data) == message {
		return true
	} else {
		return false
	}
}
