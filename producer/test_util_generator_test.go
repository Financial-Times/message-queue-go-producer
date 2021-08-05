package producer

import (
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"math/rand"
	"time"
)

func generateRandomCombinedModel() (CombinedModel, error) {
	uuid, err := generateRandomString()
	if err != nil {
		return CombinedModel{}, err
	}
	content, err := generateRandomContentModel()
	if err != nil {
		return CombinedModel{}, err
	}
	metadata, err := generateRandomAnnotations(generateRandomInt(2, 4))
	if err != nil {
		return CombinedModel{}, err
	}
	contentURI, err := generateRandomString()
	if err != nil {
		return CombinedModel{}, err
	}
	lastModified, err := generateRandomString()
	if err != nil {
		return CombinedModel{}, err
	}
	markedDeleted, err := generateRandomString()
	if err != nil {
		return CombinedModel{}, err
	}
	return CombinedModel{
		UUID:          uuid,
		Content:       content,
		Metadata:      metadata,
		ContentURI:    contentURI,
		LastModified:  lastModified,
		MarkedDeleted: markedDeleted,
	}, nil
}

func generateRandomContentModel() (ContentModel, error) {
	return generateRandomMapStrInterface(generateRandomInt(3, 5))
}

func generateRandomMapStrInterface(n int) (map[string]interface{}, error) {
	// FIXME
	var keys []string
	for i := 0; i < n; i++ {
		str, err := generateRandomString()
		if err != nil {
			return nil, err
		}
		keys = append(keys, str)
	}
	res := make(map[string]interface{})
	for _, k := range keys {
		str, err := generateRandomString()
		if err != nil {
			return nil, err
		}
		res[k] = str
	}
	return res, nil
}

func generateRandomAnnotations(n int) ([]Annotation, error) {
	var annotations []Annotation
	for i := 0; i < n; i++ {
		annotation, err := generateRandomAnnotation()
		if err != nil {
			return nil, err
		}
		annotations = append(annotations, annotation)
	}
	return annotations, nil
}

func generateRandomAnnotation() (Annotation, error) {
	thing, err := generateRandomThing()
	if err != nil {
		return Annotation{}, err
	}
	return Annotation{Thing: thing}, nil
}

func generateRandomThing() (Thing, error) {
	id, err := generateRandomString()
	if err != nil {
		return Thing{}, err
	}
	prefLabel, err := generateRandomString()
	if err != nil {
		return Thing{}, err
	}
	predicate, err := generateRandomString()
	if err != nil {
		return Thing{}, err
	}
	apiUrl, err := generateRandomString()
	if err != nil {
		return Thing{}, err
	}
	types, err := generateRandomStrings(generateRandomInt(3, 5))
	if err != nil {
		return Thing{}, err
	}
	return Thing{
		ID:        id,
		PrefLabel: prefLabel,
		Types:     types,
		Predicate: predicate,
		ApiUrl:    apiUrl,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                                                                                                                    //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func generateRandomStrings(n int) ([]string, error) {
	var strs []string
	for i := 0; i < n; i++ {
		str, err := generateRandomString()
		if err != nil {
			return nil, err
		}
		strs = append(strs, str)
	}
	return strs, nil
}

// FIXME length
func generateRandomString() (string, error) {
	hexV, err := generateRandomBytes()
	if err != nil {
		return "", err
	}
	h := sha256.New()
	if n, err := h.Write(hexV); err != nil || n != len(hexV) {
		if err != nil {
			return "", err
		}
		return "", errors.New("hashing error")
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func generateRandomBytes() ([]byte, error) {
	msg, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	hexV, err := msg.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return sha512.New().Sum(hexV), nil
}

func generateRandomInt(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max - min + 1) + min
}
