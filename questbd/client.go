package questbd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Result types

type Row struct {
	cursor  int
	Columns []string
	Dataset [][]interface{}
	count int
}

func (r *Row)Count() int {
	return r.count
}

type Results struct {
	Query string
	Count int64
}

type questDBResponse struct {
	Ddl     string
	Count   int64
	Query   string
	Columns [] struct {
		Name string
		Type string
	}
	Dataset [][]interface{}
	Error   string
}

type QuestDBRest struct {
	client  *http.Client
	baseURL *url.URL
}

func (q *QuestDBRest) connect() error {
	// Do nothing, we don't need this on the rest client
	return nil
}

func NewQuestDBRest(host string) (*QuestDBRest, error) {
	client := &http.Client{
		Timeout:time.Duration(60*time.Second),
	}
	baseUrl, err := url.Parse(host)
	if err != nil {
		return nil, err
	}
	return &QuestDBRest{
		client:  client,
		baseURL: baseUrl,
	}, nil
}

func (q *QuestDBRest) restRequest(query string) (*questDBResponse, error) {
	execRel := &url.URL{Path: "/exec"}
	endpoint := q.baseURL.ResolveReference(execRel)
	parameters := url.Values{}
	parameters.Add("query", query)
	endpoint.RawQuery = parameters.Encode()
	req, err := http.NewRequest("GET", endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := q.client.Do(req)
	defer resp.Body.Close()

	results := questDBResponse{}
	if err != nil {
		return nil, err
	}

	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(results.Error)
	}

	return &results, nil
}

func (q *QuestDBRest) Query(query string) (Row, error) {
	results, err := q.restRequest(query)
	if err != nil {
		return Row{}, err
	}
	columns := make([]string, len(results.Columns))
	for i, column := range results.Columns {
		columns[i] = column.Name
	}
	return Row{
		Dataset: results.Dataset,
		Columns: columns,
		cursor:-1,
		count: int(results.Count),
	}, nil
}

func (q *QuestDBRest) Exec(query string) (Results, error) {
	results, err := q.restRequest(query)
	if err != nil {
		return Results{}, err
	}

	return Results{
		Count: results.Count,
		Query: results.Query,
	}, nil
}

func (r *Row) Get() []interface{} {
	if r.cursor < 0 && len(r.Dataset) != 0{
		return r.Dataset[0]
	}
	return r.Dataset[r.cursor]
}

func (r *Row) Next() bool {
	r.cursor++
	return r.cursor < len(r.Dataset)

}
