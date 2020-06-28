package questbd

import (
	"encoding/base64"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/jaegertracing/jaeger/model"
	"strings"
)

// Need to url encoding, as questdb doesn't accept dot as a name of the columns
// this is not the best way to handle it..
func sanitizeTagKey(key string) string {
	result := strings.ReplaceAll(key, ".", "#")
	result = strings.ReplaceAll(result, "/", "#")
	result = strings.ReplaceAll(result, "\\", "#")
	return result
}

// This is prone to sql injection but good for demo proposes.
func escape(value interface{}) string {
	switch value.(type) {
	case int, int64, int32:
		return fmt.Sprintf("%d", value)
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%v", value),"'",""))
	default:
		return ""
	}

}

func marshallSpan(span *model.Span)  (string, error){
	bytes, err := proto.Marshal(span)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(bytes), nil
}

func unmarshallSpan(text string )  (*model.Span, error){
	bytes, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return nil, err
	}
	newSpan := &model.Span{}
	err = proto.Unmarshal(bytes, newSpan)
	return newSpan, err
}