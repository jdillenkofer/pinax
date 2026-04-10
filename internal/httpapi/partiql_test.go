package httpapi

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func TestExecuteStatementCRUDAndPagination(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("partiql_exec"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	insertStmt := `INSERT INTO "partiql_exec" VALUE {'pk': ?, 'sk': ?, 'payload': ?}`
	_, err = client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(insertStmt), Parameters: []types.AttributeValue{
		&types.AttributeValueMemberS{Value: "user#1"},
		&types.AttributeValueMemberS{Value: "order#1"},
		&types.AttributeValueMemberS{Value: "first"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(insertStmt), Parameters: []types.AttributeValue{
		&types.AttributeValueMemberS{Value: "user#1"},
		&types.AttributeValueMemberS{Value: "order#2"},
		&types.AttributeValueMemberS{Value: "second"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	selectOut, err := client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement:  aws.String(`SELECT * FROM "partiql_exec" WHERE pk = ?`),
		Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "user#1"}},
		Limit:      aws.Int32(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(selectOut.Items) != 1 {
		t.Fatalf("expected one item on first page, got %d", len(selectOut.Items))
	}
	if selectOut.NextToken == nil || *selectOut.NextToken == "" {
		t.Fatalf("expected NextToken for paginated select, got %+v", selectOut)
	}

	selectOut2, err := client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement:  aws.String(`SELECT * FROM "partiql_exec" WHERE pk = ?`),
		Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "user#1"}},
		Limit:      aws.Int32(10),
		NextToken:  selectOut.NextToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(selectOut2.Items) != 1 {
		t.Fatalf("expected second page with one item, got %d", len(selectOut2.Items))
	}

	_, err = client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement: aws.String(`UPDATE "partiql_exec" SET payload = ? WHERE pk = ? AND sk = ?`),
		Parameters: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "updated"},
			&types.AttributeValueMemberS{Value: "user#1"},
			&types.AttributeValueMemberS{Value: "order#1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	updated, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("partiql_exec"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "user#1"},
			"sk": &types.AttributeValueMemberS{Value: "order#1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if payload, ok := updated.Item["payload"].(*types.AttributeValueMemberS); !ok || payload.Value != "updated" {
		t.Fatalf("expected updated payload, got %+v", updated.Item)
	}

	_, err = client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement: aws.String(`DELETE FROM "partiql_exec" WHERE pk = ? AND sk = ?`),
		Parameters: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "user#1"},
			&types.AttributeValueMemberS{Value: "order#2"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("partiql_exec"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "user#1"},
			"sk": &types.AttributeValueMemberS{Value: "order#2"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted.Item) != 0 {
		t.Fatalf("expected deleted item to be missing, got %+v", deleted.Item)
	}
}

func TestExecuteStatementRicherWherePredicates(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("partiql_where"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	insert := `INSERT INTO "partiql_where" VALUE {'pk': ?, 'sk': ?, 'name': ?}`
	for _, row := range []struct{ sk, name string }{{"1", "alpha"}, {"2", "beta"}, {"3", "alphabet"}} {
		_, err = client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(insert), Parameters: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "u#1"},
			&types.AttributeValueMemberN{Value: row.sk},
			&types.AttributeValueMemberS{Value: row.name},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}

	out, err := client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement: aws.String(`SELECT * FROM "partiql_where" WHERE pk = ? AND sk BETWEEN ? AND ?`),
		Parameters: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "u#1"},
			&types.AttributeValueMemberN{Value: "2"},
			&types.AttributeValueMemberN{Value: "3"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 2 {
		t.Fatalf("expected 2 rows for BETWEEN, got %d", len(out.Items))
	}

	out, err = client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement:  aws.String(`SELECT * FROM "partiql_where" WHERE pk = ? AND begins_with(name, ?)`),
		Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "u#1"}, &types.AttributeValueMemberS{Value: "alph"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 2 {
		t.Fatalf("expected 2 rows for begins_with, got %d", len(out.Items))
	}

	out, err = client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement:  aws.String(`SELECT * FROM "partiql_where" WHERE pk = ? AND sk > ?`),
		Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "u#1"}, &types.AttributeValueMemberN{Value: "1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 2 {
		t.Fatalf("expected 2 rows for > predicate, got %d", len(out.Items))
	}

	out, err = client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement:  aws.String(`SELECT name AS n FROM "partiql_where" WHERE pk = ? AND sk IN (?, ?)`),
		Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "u#1"}, &types.AttributeValueMemberN{Value: "1"}, &types.AttributeValueMemberN{Value: "3"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 2 {
		t.Fatalf("expected 2 rows for IN predicate, got %d", len(out.Items))
	}
}

func TestExecuteStatementNextTokenBoundToStatement(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("partiql_tok"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}, {AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	ins := `INSERT INTO "partiql_tok" VALUE {'pk': ?, 'sk': ?}`
	for _, sk := range []string{"a", "b"} {
		_, err = client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(ins), Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "p"}, &types.AttributeValueMemberS{Value: sk}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	page, err := client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(`SELECT * FROM "partiql_tok" WHERE pk = ?`), Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "p"}}, Limit: aws.Int32(1)})
	if err != nil {
		t.Fatal(err)
	}
	if page.NextToken == nil {
		t.Fatal("expected NextToken")
	}
	_, err = client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(`SELECT * FROM "partiql_tok" WHERE pk = ? AND sk = ?`), Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "p"}, &types.AttributeValueMemberS{Value: "a"}}, NextToken: page.NextToken})
	if err == nil {
		t.Fatal("expected invalid next token error")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("expected ValidationException, got %v", err)
	}
}

func TestBatchExecuteStatementMixedResponses(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("partiql_batch"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("partiql_batch"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.BatchExecuteStatement(ctx, &dynamodb.BatchExecuteStatementInput{
		Statements: []types.BatchStatementRequest{
			{Statement: aws.String(`SELECT * FROM "partiql_batch" WHERE pk = ?`), Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "a"}}},
			{Statement: aws.String(`SELECT * FROM "partiql_batch" WHERE missing = ?`), Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "a"}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Responses) != 2 {
		t.Fatalf("expected two responses, got %+v", out.Responses)
	}
	if out.Responses[0].Item == nil {
		t.Fatalf("expected first statement item response, got %+v", out.Responses[0])
	}
	if out.Responses[1].Error == nil {
		t.Fatalf("expected second statement error response, got %+v", out.Responses[1])
	}
	if out.Responses[1].Error.Code != types.BatchStatementErrorCodeEnumValidationError {
		t.Fatalf("expected ValidationError, got %+v", out.Responses[1].Error)
	}
}

func TestBatchExecuteStatementConditionFailureReturnsOldItemWhenRequested(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("partiql_batch_cond"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("partiql_batch_cond"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}, "v": &types.AttributeValueMemberS{Value: "old"}}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := client.BatchExecuteStatement(ctx, &dynamodb.BatchExecuteStatementInput{
		Statements: []types.BatchStatementRequest{{
			Statement:                           aws.String(`UPDATE "partiql_batch_cond" SET v = ? WHERE pk = ? AND v = ?`),
			Parameters:                          []types.AttributeValue{&types.AttributeValueMemberS{Value: "new"}, &types.AttributeValueMemberS{Value: "a"}, &types.AttributeValueMemberS{Value: "mismatch"}},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Responses) != 1 || out.Responses[0].Error == nil || out.Responses[0].Error.Item == nil {
		t.Fatalf("expected conditional failure item in batch response, got %+v", out)
	}
}

func TestExecuteStatementValidationParityChecks(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(`INSERT INTO "x" VALUE {'pk': ?}`), Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "a"}}, ConsistentRead: aws.Bool(true)})
	if err == nil {
		t.Fatal("expected validation error for ConsistentRead on write")
	}
	assertAPIErrorCode(t, err, "ValidationException")

	_, err = client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(`DELETE FROM "x" WHERE pk = ?`), Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "a"}}, NextToken: aws.String("abc")})
	if err == nil {
		t.Fatal("expected validation error for NextToken on write")
	}
	assertAPIErrorCode(t, err, "ValidationException")
}

func TestExecuteTransactionAtomicBehavior(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("partiql_tx"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.ExecuteTransaction(ctx, &dynamodb.ExecuteTransactionInput{
		TransactStatements: []types.ParameterizedStatement{
			{Statement: aws.String(`INSERT INTO "partiql_tx" VALUE {'pk': ?, 'v': ?}`), Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "a"}, &types.AttributeValueMemberS{Value: "x"}}},
			{Statement: aws.String(`UPDATE "partiql_tx" SET v = ? WHERE wrong = ?`), Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "y"}, &types.AttributeValueMemberS{Value: "a"}}},
		},
	})
	if err == nil {
		t.Fatal("expected transaction cancellation error")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "TransactionCanceledException" {
		t.Fatalf("expected TransactionCanceledException, got %q", apiErr.ErrorCode())
	}
	txErr := &types.TransactionCanceledException{}
	if !errors.As(err, &txErr) {
		t.Fatalf("expected TransactionCanceledException type, got %T", err)
	}
	if len(txErr.CancellationReasons) != 2 {
		t.Fatalf("expected 2 cancellation reasons, got %+v", txErr.CancellationReasons)
	}
	if txErr.CancellationReasons[0].Code == nil || *txErr.CancellationReasons[0].Code != "None" {
		t.Fatalf("expected first reason None, got %+v", txErr.CancellationReasons[0])
	}
	if txErr.CancellationReasons[1].Code == nil || *txErr.CancellationReasons[1].Code != "ValidationError" {
		t.Fatalf("expected second reason ValidationError, got %+v", txErr.CancellationReasons[1])
	}

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("partiql_tx"), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Item) != 0 {
		t.Fatalf("expected rollback with no inserted item, got %+v", out.Item)
	}
}

func TestExecuteTransactionClientRequestTokenIdempotency(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("partiql_tx_token"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	token := aws.String("token-123")
	req := &dynamodb.ExecuteTransactionInput{
		ClientRequestToken: token,
		TransactStatements: []types.ParameterizedStatement{{
			Statement:  aws.String(`INSERT INTO "partiql_tx_token" VALUE {'pk': ?, 'v': ?}`),
			Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "a"}, &types.AttributeValueMemberS{Value: "x"}},
		}},
	}
	if _, err := client.ExecuteTransaction(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ExecuteTransaction(ctx, req); err != nil {
		t.Fatalf("expected idempotent replay to succeed, got %v", err)
	}
	_, err = client.ExecuteTransaction(ctx, &dynamodb.ExecuteTransactionInput{
		ClientRequestToken: token,
		TransactStatements: []types.ParameterizedStatement{{
			Statement:  aws.String(`INSERT INTO "partiql_tx_token" VALUE {'pk': ?, 'v': ?}`),
			Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "b"}, &types.AttributeValueMemberS{Value: "x"}},
		}},
	})
	if err == nil {
		t.Fatal("expected idempotent parameter mismatch")
	}
	assertAPIErrorCode(t, err, "IdempotentParameterMismatchException")
}
