package druid

import (
	"context"
	"errors"
	"fmt"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/rubenvp8510/godruid"
)

var (
	// ErrTraceNotFound is returned by Reader's GetTrace if no data is found for given trace ID.
	ErrTraceNotFound = errors.New("trace not found")
)

type Reader struct {
	client *godruid.Client
}

func NewReader(host string) (*Reader, error) {
	client := &godruid.Client{
		Url: host,
	}
	client.Debug = true
	return &Reader{
		client: client,
	}, nil
}

type FilterSelector map[string]string

func buildFilter(query *spanstore.TraceQueryParameters) *godruid.Filter {
	filters := make([]*godruid.Filter, 0)
	if query.DurationMax != 0 || query.DurationMin != 0 {
		max := query.DurationMax.Microseconds()
		min := query.DurationMin.Microseconds()
		filters = append(filters, godruid.FilterBound("duration", fmt.Sprintf("%d", min), fmt.Sprintf("%d", max)))
	}

	if query.OperationName != "" {
		filters = append(filters, godruid.FilterSelector("operationName", query.OperationName))
	}

	if query.ServiceName != "" {
		filters = append(filters, godruid.FilterSelector("process.serviceName", query.ServiceName))
	}

	if len(query.Tags) > 0 {
		tagFilters := make([]*godruid.Filter, 0)

		for k, v := range query.Tags {
			tagFilters = append(tagFilters, godruid.FilterSelector(tagPrefix+k, v))
		}
		tagFilter := godruid.FilterOr(tagFilters...)
		filters = append(filters, tagFilter)
	}

	return godruid.FilterAnd(filters...)
}

func (r *Reader) topNQueryBuilder(query *spanstore.TraceQueryParameters) *godruid.QueryTopN {
	druidQuery := &godruid.QueryTopN{
		DataSource: "jaeger-spans",
		Intervals: []string{
			fmt.Sprintf("%s/%s",
				query.StartTimeMin.UTC().Format("2006-01-02T15:04:05.999Z"),
				query.StartTimeMax.UTC().Format("2006-01-02T15:04:05.999Z"),
			),
		},
		Filter:    buildFilter(query),
		Dimension: godruid.DimDefault("traceId", "traceId"),
		Metric: &godruid.TopNMetric{
			Type: "dimension",
		},
		Threshold:   100,
		Granularity: godruid.GranAll,
	}
	return druidQuery
}

func (r *Reader) getTraceIds(traceQuery *spanstore.TraceQueryParameters) ([]string, error) {
	query := r.topNQueryBuilder(traceQuery)
	err := r.client.Query(query)
	println(r.client.LastRequest)
	if err != nil {
		return nil, err
	}
	traces := make([]string, 0)
	for _, res := range query.QueryResult {
		for _, r := range res.Result {
			value := r["traceId"].(string)
			if value != "" {
				traces = append(traces, value)
			}
		}

	}
	return traces, nil
}

func (r *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {

	query := &godruid.QueryScan{
		DataSource: "jaeger-spans",
		Intervals:  []string{"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"},
		Columns:    []string{"span"},
		Filter:     godruid.FilterSelector("traceId", traceID.String()),
	}

	err := r.client.Query(query)
	if err != nil {
		return nil, err
	}

	trace := &model.Trace{
		Spans: []*model.Span{},
	}

	for _, results := range query.QueryResult {
		for _, event := range results.Events {
			spanb64 := event["span"].(string)
			span, err := unmarshallSpan(spanb64)
			if err != nil {
				return nil, err
			}
			trace.Spans = append(trace.Spans, span)
		}
	}
	return trace, nil

}

func (r *Reader) getDistinctQuery(dimension, name string) *godruid.QueryTopN {
	return &godruid.QueryTopN{
		DataSource: "jaeger-spans",
		Dimension:  godruid.DimDefault(dimension, name),
		Metric: &godruid.TopNMetric{
			Type: "dimension",
		},
		Threshold:   100,
		Intervals:   []string{"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"},
		Granularity: godruid.GranAll,
	}
}

func (r *Reader) GetServices(ctx context.Context) ([]string, error) {
	query := r.getDistinctQuery("process.serviceName", "serviceName")
	err := r.client.Query(query)
	if err != nil {
		return nil, err
	}
	final := make([]string, 0)

	for _, res := range query.QueryResult {
		for _, r := range res.Result {
			value := r["serviceName"].(string)
			if value != "" {
				final = append(final, value)
			}
		}

	}
	return final, nil

}

func (r *Reader) GetOperations(ctx context.Context, traceQuery spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	query := r.getDistinctQuery("operationName", "operationName")
	err := r.client.Query(query)
	if err != nil {
		return nil, err
	}
	final := make([]spanstore.Operation, 0)

	for _, res := range query.QueryResult {
		for _, r := range res.Result {
			value := r["operationName"].(string)
			if value != "" {
				final = append(final, spanstore.Operation{
					Name: value,
				})
			}
		}

	}
	return final, nil
}

func (r *Reader) FindTraces(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) ([]*model.Trace, error) {

	traceIds, err := r.getTraceIds(traceQuery)
	if err != nil {
		return nil, err
	}

	if len(traceIds) == 0 {
		return nil, ErrTraceNotFound
	}

	traceFilters := &godruid.Filter{
		Type:"in",
		Dimension:"traceId",
		Values:traceIds,
	}

	query := &godruid.QueryScan{
		DataSource: "jaeger-spans",
		Intervals:  []string{"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"},
		Columns:    []string{"span"},
		Filter:     traceFilters,
	}
	err = r.client.Query(query)

	if err != nil {
		return nil, err
	}

	tracesMap := make(map[string]*model.Trace)

	for _, results := range query.QueryResult {
		for _, event := range results.Events {
			spanb64 := event["span"].(string)
			span, err := unmarshallSpan(spanb64)
			if err != nil {
				return nil, err
			}
			strTraceId := span.TraceID.String()
			trace, ok := tracesMap[strTraceId]
			if !ok {
				trace = &model.Trace{
					Spans: []*model.Span{},
				}
				tracesMap[strTraceId] = trace
			}
			trace.Spans = append(trace.Spans, span)
		}
	}

	traces := make([]*model.Trace, len(tracesMap))

	count := 0
	for _, trace := range tracesMap {
		traces[count] = trace
		count++
	}
	return traces, nil

}

func (r *Reader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	ids, err := r.getTraceIds(query)
	if err != nil {
		return nil, err
	}
	traceIds := make([]model.TraceID, len(ids))
	for i, id := range ids {
		trid, err := model.TraceIDFromString(id)
		if err != nil {
			return nil, err
		}
		traceIds[i] = trid
	}
	return traceIds, nil

}
