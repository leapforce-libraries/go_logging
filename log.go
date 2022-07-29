package logging

import (
	"fmt"
	"time"

	errortools "github.com/leapforce-libraries/go_errortools"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
)

// Logging
//
type Logging struct {
	BigQueryService   *bigquery.Service
	BigQueryDataset   string
	BigQueryTablename string
	Logs              []Log
}

// Log stores one action to be logged
//
type Log struct {
	Timestamp time.Time
	Operation string
	Table     string
	GUID      string
	ID        int
	OldValues string
	NewValues string
	KvK       string
	NameExact string
}

func (l *Logging) ToBigQuery() *errortools.Error {
	// get pointer to table
	sqlConfig := bigquery.SqlConfig{
		DatasetName:     l.BigQueryDataset,
		TableOrViewName: &l.BigQueryTablename,
	}
	table, errTable := l.BigQueryService.CreateTable(&sqlConfig, nil, false)
	if errTable != nil {
		return errTable
	}

	b := make([]interface{}, len(l.Logs))
	for i := range l.Logs {
		b[i] = l.Logs[i]
	}

	errInsert := l.BigQueryService.Insert(table, b)
	if errInsert != nil {
		return errInsert
	}

	return nil
}

// AddLog adds new Log instance to Logs array
//
func (l *Logging) AddLog(log Log, testMode bool) {
	if testMode {
		log.Operation = "(" + log.Operation + ")"
	}
	l.Logs = append(l.Logs, log)
}

// GetMaxTimestamp return max value of timestamp field for certain operation
//
func (l *Logging) GetMaxTimestamp(operation string, filter string) (time.Time, *errortools.Error) {
	sqlSelect := "MAX(Timestamp)"
	sqlWhere := ""
	if operation != "" {
		sqlWhere = fmt.Sprintf("Operation = '%s'", operation)
	}
	if filter != "" {
		if sqlWhere != "" {
			sqlWhere += " AND "
		}
		sqlWhere += filter
	}

	selectConfig := bigquery.SqlConfig{
		DatasetName:     l.BigQueryDataset,
		TableOrViewName: &l.BigQueryTablename,
		SqlSelect:       &sqlSelect,
		SqlWhere:        &sqlWhere,
	}

	t, e := l.BigQueryService.GetValue(&selectConfig)
	if e != nil {
		return time.Now(), e
	}

	layout := "2006-01-02 15:04:05"
	time1, _ := time.Parse(layout, "1800-01-01 00:00:00")

	// if no error but no time found in table
	if t != nil {
		_t := fmt.Sprintf("%v", *t)
		_time1, err := time.Parse(layout, _t[0:len(layout)])
		if err != nil {
			return time.Now(), errortools.ErrorMessage(err)
		}

		time1 = _time1
	}

	return time1, nil
}
