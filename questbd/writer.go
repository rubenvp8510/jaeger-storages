package questbd

import (
	"fmt"
	"github.com/jaegertracing/jaeger/model"
	"time"

	"strings"
	"sync"
)

var baseColumns = []string{
	"trace_id", "span_id", "parent_id", "operation_name", "flags", "start_time", "duration", "service_name", "span",
}

var periodPerBlock = time.Second.Nanoseconds() * 60

const (
	tagPrefix          = "__tag__prefix"
)

type Table struct {
	questDB *QuestDBRest
	name    string
	lock    sync.Mutex
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
	rows, err := t.questDB.Query(fmt.Sprintf("select start_time from %s order by start_time desc limit 1", t.name))
	if err != nil {
		println(err)
	}
	if rows.Next() {
		row := rows.Get()
		t, _ := time.Parse("2006-01-02T15:04:05.999Z", fmt.Sprintf("%v", row[0]))
		println(t.String())
		return &t
	}
	return nil
}

func (t *Table) Create(designated bool) error {
	const traceTable = "CREATE TABLE %s ( " +
		"trace_id       string," +
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
	baseColumnsCount := len(baseColumns)
	columns := make([]string, baseColumnsCount+len(tagsKeys))

	for i, col := range baseColumns {
		columns[i] = col
	}
	for i := 0; i < len(tagsKeys); i ++ {
		columns[baseColumnsCount+i] = tagsKeys[i]
	}

	t.lock.Lock()
	defer t.lock.Unlock()
	err = t.updateColumns(tagsKeys)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s ( %s ) VALUES ( %s )",
		t.name, strings.Join(columns, ","), strings.Join(values, ","))

	_, err = t.questDB.Exec(query)
	return err
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
	println(query)
	_, err := t.questDB.Exec(query)
	return err
}

type Writer struct {
	questDB         *QuestDBRest
	mainTable       *Table
	partitions      map[int]*Table
	minWritableTime *time.Time
	blocksMtx       sync.RWMutex
	nextBlock       int
	close           chan struct{}
}

func NewWriter(questDB *QuestDBRest) *Writer {
	writer := &Writer{
		questDB: questDB,
		mainTable: &Table{
			name:    "traces",
			questDB: questDB,
		},
	}
	return writer
}

func (w *Writer) sortAndTransfer() {

	var partition *Table
	w.blocksMtx.Lock()
	nextBlock := int(time.Now().UnixNano() / periodPerBlock)

	println(w.nextBlock)
	partition = w.partitions[w.nextBlock]
	fmt.Println("Partition old new:", partition)

	delete(w.partitions, w.nextBlock)
	w.nextBlock = nextBlock
	fmt.Println("Partition new:", nextBlock)

	if _, ok := w.partitions[nextBlock]; !ok {
		table :=  &Table{
			name:    fmt.Sprintf("partition_%d", nextBlock),
			questDB: w.questDB,
		}
		println("creating")
		table.CreateIfNotExist(false)
		w.partitions[nextBlock] = table

	}
	w.blocksMtx.Unlock()

	w.mainTable.lock.Lock()
	defer w.mainTable.lock.Unlock()

	partition.lock.Lock()
	defer partition.lock.Unlock()

	// Need to know what columns need to be added
	columns, _ := partition.Columns()
	err := w.mainTable.updateColumns(columns)
	if err != nil {
		println(err.Error())
		return
	}
	err = w.mainTable.InsertFrom(partition.name)
	if err != nil {
		partition.Drop()
		println(err.Error())
		return
	}

	err = partition.Drop()
	if err != nil {
		println(err.Error())
		return
	}
}

func (w *Writer) loop() {
	copyTicker := time.NewTicker(60 * time.Second)
	defer copyTicker.Stop()

	for {
		select {
		case <-copyTicker.C:
			w.sortAndTransfer()
		case <-w.close:
			return
		}
	}
}

func (w *Writer) start() {
	w.mainTable.CreateIfNotExist(true)
	blockIndex := int(time.Now().UnixNano() / periodPerBlock)
	println("Partition: ", blockIndex)
	w.partitions = make(map[int]*Table)
	w.partitions[blockIndex] = &Table{
		name:    fmt.Sprintf("partition_%d", blockIndex),
		questDB: w.questDB,
	}
	err := w.partitions[blockIndex].CreateIfNotExist(false)
	if err != nil {
		println(err)
	}
	err = w.partitions[blockIndex].Truncate()
	if err != nil {
		println(err)
	}
	w.nextBlock = blockIndex
	w.minWritableTime = nil
	w.close = make(chan struct{})
	go w.loop()

}

func (w *Writer) WriteSpan(span *model.Span) error {
	blockIndex := int(span.StartTime.UnixNano() / periodPerBlock)
	w.blocksMtx.RLock()
	block, ok := w.partitions[blockIndex]
	w.blocksMtx.RUnlock()
	if !ok {
		if blockIndex > w.nextBlock {
			w.blocksMtx.Lock()
			block = &Table{
				name:    fmt.Sprintf("partition_%d", blockIndex),
				questDB: w.questDB,
			}
			w.partitions[blockIndex] = block
			w.partitions[blockIndex].CreateIfNotExist(false)
			w.blocksMtx.Unlock()
		} else {
			return fmt.Errorf("droping span block: %d too old: %s", blockIndex, span.StartTime.String())
		}
	}
	return block.WriteSpan(span)
}
