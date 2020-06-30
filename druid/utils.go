package druid

import (
	"encoding/base64"
	"github.com/gogo/protobuf/proto"
	"github.com/jaegertracing/jaeger/model"
)

func marshallSpan(span *model.Span) (string, error) {
	bytes, err := proto.Marshal(span)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(bytes), nil
}

func unmarshallSpan(text string) (*model.Span, error) {
	bytes, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return nil, err
	}
	newSpan := &model.Span{}
	err = proto.Unmarshal(bytes, newSpan)
	return newSpan, err
}
