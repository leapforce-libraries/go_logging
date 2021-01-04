package logging

import (
	"fmt"
	"time"

	errortools "github.com/leapforce-libraries/go_errortools"
	google "github.com/leapforce-libraries/go_google"
)

// Logging
//
type Logging struct {
	BigQuery          *google.BigQuery
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
	client, errClient := l.BigQuery.CreateClient()
	if errClient != nil {
		return errClient
	}

	//guid := types.NewGUID()
	//tempTableName := "temp_" + strings.Replace(guid.String(), "-", "", -1)

	//table, errTable := l.BigQuery.CreateTable(client, l.BigQueryDataset, tempTableName, Log{}, false)
	//if errTable != nil {
	//	return errTable
	//}

	// get pointer to table
	table, errTable := l.BigQuery.CreateTable(client, l.BigQueryDataset, l.BigQueryTablename, nil, false)
	if errTable != nil {
		return errTable
	}

	b := make([]interface{}, len(l.Logs))
	for i := range l.Logs {
		b[i] = l.Logs[i]
	}

	errInsert := l.BigQuery.Insert(table, b)
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

	t, e := l.BigQuery.GetValue(l.BigQueryDataset, l.BigQueryTablename, sqlSelect, sqlWhere)
	if e != nil {
		return time.Now(), e
	}

	// if no error but no time found in table
	if t == "" {
		t = "1800-01-01 00:00:00"
	}

	layout := "2006-01-02 15:04:05"
	time1, err := time.Parse(layout, t[0:len(layout)])
	if err != nil {
		return time.Now(), errortools.ErrorMessage(err)
	}

	return time1, nil
}
