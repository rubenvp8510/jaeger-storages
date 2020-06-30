package questbd

import (
	"context"
	"errors"
	"fmt"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"strings"
)

var (
	// ErrTraceNotFound is returned by Reader's GetTrace if no data is found for given trace ID.
	ErrTraceNotFound = errors.New("trace not found")
)


const getServicesQuery = "SELECT DISTINCT service_name from traces"
const getOperationsQuery = "SELECT DISTINCT operation_name from traces"

func (w *Writer) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {

	return nil, nil
}

func (w *Writer) GetServices(ctx context.Context) ([]string, error) {
	rows, err := w.questDB.Query(getServicesQuery)
	if err != nil {
		return nil, err
	}

	services := make([]string, rows.Count())
	count := 0
	for rows.Next() {
		row := rows.Get()
		service := row[0]
		services[count] = fmt.Sprintf("%v", service)
		count++
	}
	return services, nil

}

func (w *Writer) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	rows, err := w.questDB.Query(getOperationsQuery)
	if err != nil {
		return nil, err
	}
	operations := make([]spanstore.Operation, rows.Count())
	count := 0
	for rows.Next() {
		row := rows.Get()
		operations[count].Name = fmt.Sprintf("%v", row[0])
		count++
	}
	return operations, nil
}

func (w *Writer) buildQueryCondition(query *spanstore.TraceQueryParameters) (string, bool) {
	var conditions []string
	if query.DurationMax != 0 || query.DurationMin != 0 {
		max := query.DurationMax.Microseconds()
		min := query.DurationMin.Microseconds()
		conditions = append(conditions, fmt.Sprintf(" duration <= %d AND duration >= %d ", max, min))
	}
	startTimeMax := query.StartTimeMax.UTC().Format("2006-01-02T15:04:05.999Z")
	startTimeMin := query.StartTimeMin.UTC().Format("2006-01-02T15:04:05.999Z")

	conditions = append(conditions, fmt.Sprintf(" start_time <= %s AND start_time >= %s ", escape(startTimeMax), escape(startTimeMin)))

	if query.OperationName != "" {
		conditions = append(conditions, fmt.Sprintf(" operation_name = %s ", escape(query.OperationName)))
	}

	if query.ServiceName != "" {
		conditions = append(conditions, fmt.Sprintf(" service_name = %s ", escape(query.ServiceName)))
	}

	if len(query.Tags) > 0{
		var tags []string
		var tagMap map[string]string
		for key, value := range query.Tags {
			tags = append(tags,  sanitizeTagKey(key))
			tagMap[key]=  escape(value)
		}

		tagsQuery := fmt.Sprintf("SELECT column FROM table_columns(traces) where column IN ( %s )", strings.Join(tags,","))
		tagRows, _ := w.questDB.Query(tagsQuery)

		if tagRows.Count() == 0 {
			// No results, so we return false indicating premature results will be empty
			return strings.Join(conditions, " AND "), false
		}
		for tagRows.Next() {
			row := fmt.Sprintf("%v",tagRows.Get()[0])
			conditions = append(conditions, fmt.Sprintf(" %s = %s", row, tagMap[row]))
		}
	}
	return strings.Join(conditions, " AND "), true
}

func (w *Writer) findTraceIds(query *spanstore.TraceQueryParameters) ([]string, error) {
	condition, hasResults := w.buildQueryCondition(query)
	if !hasResults {
		return []string{}, nil
	}
	selectQuery := fmt.Sprintf("SELECT DISTINCT trace_id FROM traces WHERE %s", condition)
	rows, err := w.questDB.Query(selectQuery)
	if err != nil {
		return []string{}, err
	}
	ids := make([]string, rows.Count())
	count := 0
	for rows.Next() {
		traceRow := rows.Get()
		traceId := fmt.Sprintf("%v", traceRow[0])
		ids[count] = traceId
		count++
	}
	return ids, nil
}

func (w *Writer) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	traceIds, err := w.findTraceIds(query)
	if err != nil {
		return nil, err
	}
	tracesMap := make(map[string]*model.Trace, len(traceIds))
	for index, traceId := range traceIds {
		tracesMap[traceId] = &model.Trace{}
		traceIds[index] = escape(traceIds[index])
	}
	if len(traceIds) <= 0 {
		return []*model.Trace{}, nil
	}

	selectQuery := fmt.Sprintf("SELECT  trace_id, span FROM traces WHERE trace_id IN ( %s )", strings.Join(traceIds, ","))
	rows, err := w.questDB.Query(selectQuery)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		traceRow := rows.Get()
		traceId := fmt.Sprintf("%v", traceRow[0])
		spanString := fmt.Sprintf("%v", traceRow[1])
		span, err := unmarshallSpan(spanString)
		if err != nil {
			return nil, err
		}
		trace, _ := tracesMap[traceId]
		trace.Spans = append(trace.Spans, span)

	}
	traces := make([]*model.Trace, len(tracesMap))

	count := 0
	for _, trace := range tracesMap {
		traces[count] = trace
		count++
	}
	return traces, nil
}

func (w *Writer) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	traceIdsStr, err := w.findTraceIds(query)
	if err != nil {
		return []model.TraceID{}, err
	}
	traceids := make([]model.TraceID, len(traceIdsStr))
	for index, traceId := range traceIdsStr {
		traceId, err := model.TraceIDFromString(traceId)
		if err != nil {
			return []model.TraceID{}, err
		}
		traceids[index] = traceId
	}
	return traceids, nil

}
