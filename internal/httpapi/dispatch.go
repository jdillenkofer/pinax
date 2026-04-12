package httpapi

import (
	"net/http"

	"github.com/jdillenkofer/pinax/internal/awserr"
)

type operationHandler func(*Server, *http.Request, []byte) (map[string]any, error)

var operationHandlers = map[string]operationHandler{
	"CreateTable":               (*Server).createTable,
	"DescribeTable":             (*Server).describeTable,
	"DescribeLimits":            (*Server).describeLimits,
	"DescribeEndpoints":         (*Server).describeEndpoints,
	"ListTables":                (*Server).listTables,
	"DeleteTable":               (*Server).deleteTable,
	"UpdateTable":               (*Server).updateTable,
	"PutItem":                   (*Server).putItem,
	"GetItem":                   (*Server).getItem,
	"DeleteItem":                (*Server).deleteItem,
	"UpdateItem":                (*Server).updateItem,
	"Query":                     (*Server).query,
	"Scan":                      (*Server).scan,
	"BatchGetItem":              (*Server).batchGetItem,
	"BatchWriteItem":            (*Server).batchWriteItem,
	"TransactGetItems":          (*Server).transactGetItems,
	"TransactWriteItems":        (*Server).transactWriteItems,
	"UpdateTimeToLive":          (*Server).updateTimeToLive,
	"DescribeTimeToLive":        (*Server).describeTimeToLive,
	"CreateBackup":              (*Server).createBackup,
	"DescribeBackup":            (*Server).describeBackup,
	"ListBackups":               (*Server).listBackups,
	"DeleteBackup":              (*Server).deleteBackup,
	"RestoreTableFromBackup":    (*Server).restoreTableFromBackup,
	"UpdateContinuousBackups":   (*Server).updateContinuousBackups,
	"DescribeContinuousBackups": (*Server).describeContinuousBackups,
	"RestoreTableToPointInTime": (*Server).restoreTableToPointInTime,
	"TagResource":               (*Server).tagResource,
	"UntagResource":             (*Server).untagResource,
	"ListTagsOfResource":        (*Server).listTagsOfResource,
	"PutResourcePolicy":         (*Server).putResourcePolicy,
	"GetResourcePolicy":         (*Server).getResourcePolicy,
	"DeleteResourcePolicy":      (*Server).deleteResourcePolicy,
	"ExecuteStatement":          (*Server).executeStatement,
	"BatchExecuteStatement":     (*Server).batchExecuteStatement,
	"ExecuteTransaction":        (*Server).executeTransaction,
	"ListStreams":               (*Server).listStreams,
	"DescribeStream":            (*Server).describeStream,
	"GetShardIterator":          (*Server).getShardIterator,
	"GetRecords":                (*Server).getRecords,
}

func (s *Server) dispatch(r *http.Request, operation string, body []byte) (map[string]any, error) {
	handler, ok := operationHandlers[operation]
	if !ok {
		return nil, awserr.Validation("unsupported operation " + operation)
	}
	return handler(s, r, body)
}
