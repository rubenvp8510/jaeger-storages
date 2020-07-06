package questbd

import (
	"bytes"
	"fmt"
	"github.com/jaegertracing/jaeger/model"
	"strings"
	"sync"
	"time"
)

var baseColumns = []string{
	"trace_id", "span_id", "parent_id", "operation_name", "flags", "start_time", "duration", "service_name", "span",
}

var periodPerBlock = time.Second.Nanoseconds() * 60

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

const (
	tagPrefix = "__tag__prefix"
)

type Table struct {
	sync.RWMutex
	questDB *QuestDBRest
	name    string
	lock    sync.Mutex
	buffer  *bytes.Buffer
}

func (t *Table) Columns() ([]string, error) {
	query := fmt.Sprintf("SELECT column FROM table_columns('%s')", t.name)

	rows, err := t.questDB.Query(query)
	if err != nil {
		return []string{}, err
	}
	columns := make([]string, rows.Count())
	counter := 0
	for rows.Next() {
		columns[counter] = fmt.Sprintf("%v", rows.Get()[0])
		counter++
	}
	return columns, nil
}

func (t *Table) NeedToCreate(columns []string) ([]string, error) {
	const queryTemplate = "SELECT column FROM table_columns('%s') where column IN ( %s );"
	quotedColumns := make([]string, len(columns))

	for i, column := range columns {
		quotedColumns[i] = fmt.Sprintf("'%s'", column)
	}

	query := fmt.Sprintf(queryTemplate, t.name, strings.Join(quotedColumns, ","))
	rows, err := t.questDB.Query(query)

	if err != nil {
		return []string{}, err
	}

	mb := make(map[string]struct{}, rows.Count())
	for rows.Next() {
		mb[fmt.Sprintf("%v", rows.Get()[0])] = struct{}{}
	}

	var needAdd []string

	for _, column := range columns {
		if _, found := mb[column]; !found {
			needAdd = append(needAdd, column)
		}
	}

	return needAdd, nil
}

func (t *Table) createColumns(columns [] string) error {
	const AddColumnsQuery = "ALTER TABLE %s ADD COLUMN %s"
	if len(columns) > 0 {
		quotedColumns := make([]string, len(columns))
		for i, key := range columns {
			quotedColumns[i] = fmt.Sprintf("%s STRING", key)
		}
		addTagsQuery := fmt.Sprintf(AddColumnsQuery, t.name, strings.Join(quotedColumns, " , "))
		_, err := t.questDB.Exec(addTagsQuery)
		return err
	}
	return nil
}

func (t *Table) updateColumns(columns [] string) error {
	newColumns, err := t.NeedToCreate(columns)
	if err != nil {
		return err
	}
	return t.createColumns(newColumns)
}

func (t *Table) getLatest() *time.Time {
	rows, _ := t.questDB.Query(fmt.Sprintf("select start_time from %s order by start_time desc limit 1", t.name))
	if rows.Next() {
		row := rows.Get()
		t, _ := time.Parse("2006-01-02T15:04:05.999Z", fmt.Sprintf("%v", row[0]))
		return &t
	}
	return nil
}

func (t *Table) Create(designated bool) error {
	const traceTable = "CREATE TABLE %s ( " +
		"trace_id       symbol index," +
		"span_id        long," +
		"parent_id      long," +
		"operation_name string," +
		"service_name   symbol," +
		"flags          int," +
		"start_time     timestamp," +
		"duration       int,  " +
		"span           string   " +
		") %s"

	timestamp := ""
	if designated {
		timestamp = " timestamp(start_time)"
	}

	_, err := t.questDB.Exec(fmt.Sprintf(traceTable, t.name, timestamp))
	return err
}

func (t *Table) Drop() error {
	query := fmt.Sprintf("DROP TABLE %s", t.name)
	_, err := t.questDB.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (t *Table) Truncate() error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", t.name)
	_, err := t.questDB.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (t *Table) Flush() {
	t.Lock()
	oldBuf := t.buffer
	t.buffer = bufPool.Get().(*bytes.Buffer)
	t.buffer.Reset()
	t.Unlock()
	go t.writeToStorage(oldBuf)
}

func (t *Table) writeToStorage(buf *bytes.Buffer) {

	content := string(buf.Bytes())
	items := strings.Split(content, "\n")
	for _, item := range items {
		item_parts := strings.Split(item, ";")
		tagsKeys := strings.Split(item_parts[0], ",")
		values := item_parts[1]

		baseColumnsCount := len(baseColumns)
		columns := make([]string, baseColumnsCount+len(tagsKeys))

		for i, col := range baseColumns {
			columns[i] = col
		}
		for i := 0; i < len(tagsKeys); i ++ {
			columns[baseColumnsCount+i] = tagsKeys[i]
		}
		t.lock.Lock()
		err := t.updateColumns(tagsKeys)
		if err != nil {
			t.lock.Unlock()
			continue
		}

		query := fmt.Sprintf("INSERT INTO %s ( %s ) VALUES ( %s )",
			t.name, strings.Join(columns, ","), values)

		_, err = t.questDB.Exec(query)
		t.lock.Unlock()
	}

	buf.Reset()
	bufPool.Put(buf)
}

func (t *Table) WriteSpan(span *model.Span) error {

	// deduplication.
	tagsMap := make(map[string]string, len(span.Tags))
	for _, tag := range span.Tags {
		key := fmt.Sprintf("%s_%s", tagPrefix, sanitizeTagKey(tag.Key))
		tagsMap[key] = escape(tag.AsString())
	}

	tagsKeys := make([]string, len(tagsMap))
	tagValues := make([]string, len(tagsMap))

	// remove duplitaced keys

	counter := 0
	for key, value := range tagsMap {
		tagsKeys[counter] = key
		tagValues[counter] = value
		counter++
	}

	var values []string
	serializedSpan, err := marshallSpan(span)
	if err != nil {
		return err
	}

	values = append(values,
		escape(span.TraceID.String()),
		escape(int64(span.SpanID)),
		escape(int64(span.ParentSpanID())),
		escape(span.OperationName),
		escape(int32(span.Flags)),
		escape(span.StartTime.UnixNano()/1000),
		escape(span.Duration.Microseconds()),
		escape(span.Process.ServiceName),
		escape(serializedSpan),
	)
	values = append(values, tagValues...)
	buff := []byte(fmt.Sprintf("%s;%s\n", strings.Join(tagsKeys, ","), strings.Join(values, ",")))
	t.Lock()
	_, err = t.buffer.Write(buff)
	if err != nil {
		t.Unlock()
		return err
	}
	t.Unlock()
	return nil
}

func (t *Table) Exist() (bool, error) {
	rows, err := t.questDB.Query("SHOW TABLES")
	if err != nil {
		return false, err
	}
	for rows.Next() {
		row := rows.Get()
		table := fmt.Sprintf("%v", row[0])
		if table == t.name {
			return true, nil
		}
	}
	return false, nil
}

func (t *Table) CreateIfNotExist(designated bool) error {
	exist, err := t.Exist()
	if err != nil {
		return err
	}
	if !exist {
		return t.Create(designated)
	}
	return nil
}

func (t *Table) InsertFrom(table string) error {
	const transferQuery = "INSERT INTO %s SELECT * FROM (%s ORDER BY start_time)"
	query := fmt.Sprintf(transferQuery, t.name, table)
	_, err := t.questDB.Exec(query)
	return err
}
