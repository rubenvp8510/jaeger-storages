package druid

import (
	"encoding/json"
	"github.com/jaegertracing/jaeger/model"
)

const tagPrefix = "__tag."

type DruidMarshall struct {

}

func (m *DruidMarshall) Marshal(span *model.Span) ([]byte, error) {
	normalizedSpan := map[string]interface{}{}
	normalizedSpan["traceId"] = span.TraceID.String()
	normalizedSpan["spanID"] = span.SpanID.String()
	normalizedSpan["operationName"] = span.OperationName
	normalizedSpan["flags"] = uint32(span.Flags)
	normalizedSpan["startTime"] = span.StartTime
	normalizedSpan["duration"] = span.Duration.Microseconds()
	normalizedSpan["process.serviceName"] = span.Process.ServiceName
	normalizedSpan["process.processId"] = span.ProcessID
	bytes, err :=  marshallSpan(span)
	if err != nil {
		return nil, err
	}
	normalizedSpan["span"] = bytes
	for _, tag := range span.Tags  {
		normalizedSpan[tagPrefix+tag.Key] = tag.AsString()
	}
	return json.Marshal(normalizedSpan)
}

