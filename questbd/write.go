package questbd

import (
	"bytes"
	"fmt"
	"github.com/jaegertracing/jaeger/model"
	"time"

	"sync"
)

type Writer struct {
	questDB         *QuestDBRest
	mainTable       *Table
	partitions      map[int]*Table
	minWritableTime *time.Time
	blocksMtx       sync.RWMutex
	nextBlock       int
	numSpansMtx     sync.Mutex
	numSpans        int
	close           chan struct{}
}

func NewWriter(questDB *QuestDBRest) *Writer {
	writer := &Writer{
		questDB: questDB,
		mainTable: &Table{
			name:    "traces",
			questDB: questDB,
			buffer:  bytes.NewBuffer(nil),
		},
	}
	return writer
}

func (w *Writer) sortAndTransfer() {

	var partition *Table
	w.blocksMtx.Lock()
	nextBlock := int(time.Now().UnixNano() / periodPerBlock)

	partition = w.partitions[w.nextBlock]

	delete(w.partitions, w.nextBlock)
	w.nextBlock = nextBlock

	if _, ok := w.partitions[nextBlock]; !ok {
		table := &Table{
			name:    fmt.Sprintf("partition_%d", nextBlock),
			questDB: w.questDB,
		}
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
	w.mainTable.CreateIfNotExist(false)
	blockIndex := int(time.Now().UnixNano() / periodPerBlock)
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
	//go w.loop()

}

func (w *Writer) WriteSpan(span *model.Span) error {
	if err := w.mainTable.WriteSpan(span); err != nil {
		return err
	}
	w.numSpansMtx.Lock()
	w.numSpans++
	if w.numSpans >= 1024 {
		w.mainTable.Flush()
		w.numSpans = 0
	}
	w.numSpansMtx.Unlock()
	return nil
}
