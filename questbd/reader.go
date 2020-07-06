package questbd

import (
	"context"
	"errors"
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
		services[count] = service.(string)
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
		operations[count].Name = (row[0]).(string)
		count++
	}
	return operations, nil
}

func (w *Writer) buildQueryCondition(query *spanstore.TraceQueryParameters) (string, bool) {
	var conditions []string
	if query.DurationMax != 0 || query.DurationMin != 0 {
		max := query.DurationMax.Microseconds()
		min := query.DurationMin.Microseconds()
		conditions = append(conditions, " duration <= "+string(max)+" AND duration >=  "+string(min))
	}
	startTimeMax := query.StartTimeMax.UTC().Format("2006-01-02T15:04:05.999Z")
	startTimeMin := query.StartTimeMin.UTC().Format("2006-01-02T15:04:05.999Z")

	conditions = append(conditions, " start_time <= "+escape(startTimeMax)+" AND start_time >= "+escape(startTimeMin))

	if query.OperationName != "" {
		conditions = append(conditions, " operation_name = "+escape(query.OperationName))
	}

	if query.ServiceName != "" {
		conditions = append(conditions, " service_name = "+escape(query.ServiceName))
	}

	if len(query.Tags) > 0 {
		var tags []string
		tagMap := make(map[string]string, len(query.Tags))
		for key, value := range query.Tags {
			tags = append(tags, escape(sanitizeTagKey(tagPrefix+key)))
			tagMap[key] = escape(value)
		}

		tagsQuery := "SELECT column FROM table_columns('traces') where column IN ( " + strings.Join(tags, ",") + " )"

		tagRows, err := w.questDB.Query(tagsQuery)

		if err != nil {
			println(err.Error())
		}
		if tagRows.Count() == 0 {
			// No results, so we return false indicating premature results will be empty
			return strings.Join(conditions, " AND "), false
		}
		for tagRows.Next() {
			row := (tagRows.Get()[0]).(string)
			conditions = append(conditions, row+"="+tagMap[row])
		}
	}
	return strings.Join(conditions, " AND "), true
}

func (w *Writer) findTraceIdsQuery(query *spanstore.TraceQueryParameters) string {
	condition, hasResults := w.buildQueryCondition(query)
	if !hasResults {
		return ""
	}
	selectQuery := "SELECT DISTINCT trace_id FROM traces timestamp(start_time) WHERE " + condition
	return selectQuery
}

func (w *Writer) findTraceIds(query *spanstore.TraceQueryParameters) ([]string, error) {
	condition, hasResults := w.buildQueryCondition(query)
	if !hasResults {
		return []string{}, nil
	}

	selectQuery := "SELECT DISTINCT trace_id FROM traces timestamp(start_time) WHERE " + condition
	rows, err := w.questDB.Query(selectQuery)
	if err != nil {
		return []string{}, err
	}
	ids := make([]string, rows.Count())
	count := 0
	for rows.Next() {
		traceRow := rows.Get()
		traceId := (traceRow[0]).(string)
		ids[count] = traceId
		count++
	}
	return ids, nil
}

func (w *Writer) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {

	subQuery := w.findTraceIdsQuery(query)
	if subQuery == "" {
		return []*model.Trace{}, nil
	}

	selectQuery := "SELECT  trace_id, span FROM traces WHERE trace_id IN (" + subQuery + " )"
	rows, err := w.questDB.Query(selectQuery)

	if err != nil {
		return nil, err
	}
	tracesMap := make(map[string]*model.Trace, 50)

	for rows.Next() {
		traceRow := rows.Get()
		traceId := traceRow[0].(string)
		spanString := traceRow[1].(string)
		span, err := unmarshallSpan(spanString)
		if err != nil {
			return nil, err
		}
		trace, ok := tracesMap[traceId]
		if !ok {
			trace = &model.Trace{}
		}
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
