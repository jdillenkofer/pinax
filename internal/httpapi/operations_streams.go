package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/awserr"
	"github.com/jdillenkofer/pinax/internal/model"
)

type listStreamsRequest struct {
	TableName               string `json:"TableName"`
	Limit                   int    `json:"Limit"`
	ExclusiveStartStreamARN string `json:"ExclusiveStartStreamArn"`
}

type describeStreamRequest struct {
	StreamARN             string `json:"StreamArn"`
	Limit                 int    `json:"Limit"`
	ExclusiveStartShardID string `json:"ExclusiveStartShardId"`
}

type getShardIteratorRequest struct {
	StreamARN         string `json:"StreamArn"`
	ShardID           string `json:"ShardId"`
	ShardIteratorType string `json:"ShardIteratorType"`
	SequenceNumber    string `json:"SequenceNumber"`
}

type getRecordsRequest struct {
	ShardIterator string `json:"ShardIterator"`
	Limit         int    `json:"Limit"`
}

type streamIteratorToken struct {
	StreamARN string `json:"streamArn"`
	ShardID   string `json:"shardId"`
	Sequence  int64  `json:"sequence"`
	ExpiresAt int64  `json:"expiresAt"`
}

func (s *Server) listStreams(r *http.Request, body []byte) (map[string]any, error) {
	var req listStreamsRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	limit := req.Limit
	if limit < 0 {
		return nil, awserr.Validation("Limit must be greater than or equal to 0")
	}
	if limit <= 0 {
		limit = defaultListStreamsLimit
	}

	streams := make([]map[string]any, 0)
	start := ""
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		for {
			names, err := repos.Tables().ListTables(txCtx, start, defaultListStreamsLimit)
			if err != nil {
				return err
			}
			if len(names) == 0 {
				break
			}
			for _, name := range names {
				storedAccountID, storedTableName := splitScopedTableKey(name)
				if storedAccountID != accountIDFromContext(txCtx) {
					continue
				}
				if strings.TrimSpace(req.TableName) != "" && storedTableName != req.TableName {
					continue
				}
				t, err := s.getTableWithLifecycleByKey(txCtx, repos.Tables(), name)
				if err != nil {
					continue
				}
				if !t.Stream.Enabled || strings.TrimSpace(t.Stream.ARN) == "" {
					continue
				}
				if strings.TrimSpace(req.ExclusiveStartStreamARN) != "" && t.Stream.ARN <= req.ExclusiveStartStreamARN {
					continue
				}
				streams = append(streams, map[string]any{
					"StreamArn":   t.Stream.ARN,
					"TableName":   logicalTableNameFromKey(t.Name),
					"StreamLabel": t.Stream.Label,
				})
			}
			start = names[len(names)-1]
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(streams, func(i, j int) bool {
		return streams[i]["StreamArn"].(string) < streams[j]["StreamArn"].(string)
	})
	truncated := streams
	if len(truncated) > limit {
		truncated = truncated[:limit]
	}
	resp := map[string]any{"Streams": truncated}
	if len(streams) > limit {
		resp["LastEvaluatedStreamArn"] = truncated[len(truncated)-1]["StreamArn"]
	}

	return resp, nil
}

func (s *Server) describeStream(r *http.Request, body []byte) (map[string]any, error) {
	var req describeStreamRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.StreamARN) == "" {
		return nil, awserr.Validation("StreamArn is required")
	}
	limit := req.Limit
	if limit < 0 {
		return nil, awserr.Validation("Limit must be greater than or equal to 0")
	}
	if limit <= 0 {
		limit = defaultDescribeStreamLimit
	}

	var (
		t        model.Table
		firstSeq int64
		found    bool
	)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var txErr error
		t, txErr = s.getTableByStreamARNFromRepo(txCtx, repos.Tables(), req.StreamARN)
		if txErr != nil {
			return txErr
		}
		firstSeq, _, found, txErr = repos.Streams().GetStreamSequenceBounds(txCtx, req.StreamARN)
		if txErr != nil {
			return txErr
		}
		return nil
	}); err != nil {
		return nil, err
	}
	starting := "0"
	if found {
		starting = strconv.FormatInt(firstSeq, 10)
	}
	shards := []map[string]any{}
	if strings.TrimSpace(req.ExclusiveStartShardID) == "" {
		shards = append(shards, map[string]any{
			"ShardId": streamDefaultShardID,
			"SequenceNumberRange": map[string]any{
				"StartingSequenceNumber": starting,
			},
		})
	}
	if len(shards) > limit {
		shards = shards[:limit]
	}
	resp := map[string]any{
		"StreamDescription": map[string]any{
			"StreamArn":               t.Stream.ARN,
			"StreamStatus":            "ENABLED",
			"StreamViewType":          t.Stream.ViewType,
			"CreationRequestDateTime": t.CreatedAt,
			"TableName":               logicalTableNameFromKey(t.Name),
			"KeySchema":               t.KeySchema(),
			"Shards":                  shards,
		},
	}
	return resp, nil
}

func (s *Server) getShardIterator(r *http.Request, body []byte) (map[string]any, error) {
	var req getShardIteratorRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.StreamARN) == "" {
		return nil, awserr.Validation("StreamArn is required")
	}
	if strings.TrimSpace(req.ShardID) == "" {
		return nil, awserr.Validation("ShardId is required")
	}
	if req.ShardID != streamDefaultShardID {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	iteratorType := strings.TrimSpace(req.ShardIteratorType)
	if iteratorType == "" {
		return nil, awserr.Validation("ShardIteratorType is required")
	}
	if (iteratorType == "TRIM_HORIZON" || iteratorType == "LATEST") && strings.TrimSpace(req.SequenceNumber) != "" {
		return nil, awserr.Validation("SequenceNumber is not valid for this ShardIteratorType")
	}

	last := int64(0)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		if _, err := s.getTableByStreamARNFromRepo(txCtx, repos.Tables(), req.StreamARN); err != nil {
			return err
		}
		_, seqLast, found, err := repos.Streams().GetStreamSequenceBounds(txCtx, req.StreamARN)
		if err != nil {
			return err
		}
		if found {
			last = seqLast
		}
		return nil
	}); err != nil {
		return nil, err
	}

	sequence := int64(0)
	switch iteratorType {
	case "TRIM_HORIZON":
		sequence = 0
	case "LATEST":
		sequence = last
	case "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER":
		if strings.TrimSpace(req.SequenceNumber) == "" {
			return nil, awserr.Validation("SequenceNumber is required for this ShardIteratorType")
		}
		parsed, err := strconv.ParseInt(strings.TrimSpace(req.SequenceNumber), 10, 64)
		if err != nil || parsed < 0 {
			return nil, awserr.Validation("Invalid SequenceNumber")
		}
		if iteratorType == "AT_SEQUENCE_NUMBER" {
			sequence = parsed - 1
		} else {
			sequence = parsed
		}
	default:
		return nil, awserr.Validation("Invalid ShardIteratorType")
	}
	if sequence < 0 {
		sequence = 0
	}
	iter, err := s.encodeStreamIterator(streamIteratorToken{
		StreamARN: req.StreamARN,
		ShardID:   req.ShardID,
		Sequence:  sequence,
		ExpiresAt: time.Now().Add(s.streamIteratorTTL).UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"ShardIterator": iter}, nil
}

func (s *Server) getRecords(r *http.Request, body []byte) (map[string]any, error) {
	var req getRecordsRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.ShardIterator) == "" {
		return nil, awserr.Validation("ShardIterator is required")
	}
	limit := req.Limit
	if limit < 0 {
		return nil, awserr.Validation("Limit must be greater than or equal to 0")
	}
	if limit <= 0 {
		limit = defaultStreamReadLimit
	}
	if limit > 1000 {
		return nil, awserr.Validation("Limit must be less than or equal to 1000")
	}
	token, err := s.decodeStreamIterator(req.ShardIterator)
	if err != nil {
		return nil, awserr.Validation("Invalid ShardIterator")
	}
	if token.ExpiresAt <= time.Now().UnixMilli() {
		return nil, awserr.ExpiredIterator("Shard iterator has expired")
	}

	var (
		records         []model.StreamRecord
		latestChangedAt int64
	)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		if _, err := s.getTableByStreamARNFromRepo(txCtx, repos.Tables(), token.StreamARN); err != nil {
			return err
		}
		var err error
		records, err = repos.Streams().ListStreamRecordsAfterSequence(txCtx, token.StreamARN, token.Sequence, limit)
		if err != nil {
			return err
		}
		_, last, found, err := repos.Streams().GetStreamSequenceBounds(txCtx, token.StreamARN)
		if err != nil {
			return err
		}
		if !found {
			last = token.Sequence
		}
		if found {
			latestChangedAt, _, err = repos.Streams().GetStreamRecordChangedAt(txCtx, token.StreamARN, last)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	outRecords := make([]map[string]any, 0, len(records))
	nextSeq := token.Sequence
	bytesUsed := 0
	for _, record := range records {
		recordSize := itemSizeOrZero(record.Keys) + itemSizeOrZero(record.OldImage) + itemSizeOrZero(record.NewImage)
		if bytesUsed+recordSize > streamResponseMaxBytes && len(outRecords) > 0 {
			break
		}
		outRecords = append(outRecords, streamRecordResponse(record))
		nextSeq = record.Sequence
		bytesUsed += recordSize
	}
	behind := int64(0)
	if latestChangedAt > 0 {
		if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
			cursorChangedAt, cursorFound, err := repos.Streams().GetStreamRecordChangedAt(txCtx, token.StreamARN, nextSeq)
			if err != nil {
				return err
			}
			if !cursorFound {
				cursorChangedAt = time.Now().UnixMilli()
			}
			if latestChangedAt > cursorChangedAt {
				behind = latestChangedAt - cursorChangedAt
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	nextIterator, err := s.encodeStreamIterator(streamIteratorToken{
		StreamARN: token.StreamARN,
		ShardID:   token.ShardID,
		Sequence:  nextSeq,
		ExpiresAt: time.Now().Add(s.streamIteratorTTL).UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"Records":            outRecords,
		"NextShardIterator":  nextIterator,
		"MillisBehindLatest": behind,
	}, nil
}

type streamIteratorEnvelope struct {
	Token streamIteratorToken `json:"token"`
	Sig   string              `json:"sig"`
}

func (s *Server) encodeStreamIterator(token streamIteratorToken) (string, error) {
	raw, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	sig := hmac.New(sha256.New, s.streamIteratorSigningKey)
	_, _ = sig.Write(raw)
	envelopeRaw, err := json.Marshal(streamIteratorEnvelope{
		Token: token,
		Sig:   hex.EncodeToString(sig.Sum(nil)),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(envelopeRaw), nil
}

func (s *Server) decodeStreamIterator(raw string) (streamIteratorToken, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return streamIteratorToken{}, err
	}
	var envelope streamIteratorEnvelope
	if err := json.Unmarshal(decoded, &envelope); err != nil {
		return streamIteratorToken{}, err
	}
	rawToken, err := json.Marshal(envelope.Token)
	if err != nil {
		return streamIteratorToken{}, err
	}
	sig := hmac.New(sha256.New, s.streamIteratorSigningKey)
	_, _ = sig.Write(rawToken)
	expectedSig := hex.EncodeToString(sig.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(expectedSig), []byte(envelope.Sig)) != 1 {
		return streamIteratorToken{}, fmt.Errorf("invalid shard iterator")
	}
	token := envelope.Token
	if strings.TrimSpace(token.StreamARN) == "" || strings.TrimSpace(token.ShardID) == "" {
		return streamIteratorToken{}, fmt.Errorf("invalid shard iterator")
	}
	return token, nil
}

func streamRecordResponse(record model.StreamRecord) map[string]any {
	dynamodbRecord := map[string]any{
		"ApproximateCreationDateTime": float64(record.ChangedAt) / 1000,
		"SequenceNumber":              strconv.FormatInt(record.Sequence, 10),
		"StreamViewType":              streamViewTypeForRecord(record),
		"Keys":                        record.Keys,
		"SizeBytes":                   itemSizeOrZero(record.Keys) + itemSizeOrZero(record.OldImage) + itemSizeOrZero(record.NewImage),
	}
	if record.NewImage != nil {
		dynamodbRecord["NewImage"] = record.NewImage
	}
	if record.OldImage != nil {
		dynamodbRecord["OldImage"] = record.OldImage
	}
	return map[string]any{
		"eventID":        strconv.FormatInt(record.Sequence, 10),
		"eventName":      record.EventName,
		"eventVersion":   "1.1",
		"eventSource":    "aws:dynamodb",
		"awsRegion":      "local",
		"eventSourceARN": record.StreamARN,
		"dynamodb":       dynamodbRecord,
	}
}

func itemSizeOrZero(item map[string]any) int {
	if item == nil {
		return 0
	}
	return model.CalculateItemSizeBytes(item)
}

func streamViewTypeForRecord(record model.StreamRecord) string {
	if record.NewImage != nil && record.OldImage != nil {
		return "NEW_AND_OLD_IMAGES"
	}
	if record.NewImage != nil {
		return "NEW_IMAGE"
	}
	if record.OldImage != nil {
		return "OLD_IMAGE"
	}
	return "KEYS_ONLY"
}

func (s *Server) getTableByStreamARNFromRepo(ctx context.Context, tables uow.TableRepo, streamARN string) (model.Table, error) {
	name, err := tableNameFromStreamARN(streamARN)
	if err != nil {
		return model.Table{}, awserr.ResourceNotFound("Requested resource not found")
	}
	t, err := s.getTableWithLifecycleFromRepo(ctx, tables, name)
	if err != nil {
		return model.Table{}, awserr.ResourceNotFound("Requested resource not found")
	}
	if !t.Stream.Enabled || t.Stream.ARN != strings.TrimSpace(streamARN) {
		return model.Table{}, awserr.ResourceNotFound("Requested resource not found")
	}
	return t, nil
}

func tableNameFromStreamARN(streamARN string) (string, error) {
	streamARN = strings.TrimSpace(streamARN)
	if !strings.HasPrefix(streamARN, "arn:") {
		return "", fmt.Errorf("invalid stream arn")
	}
	marker := ":table/"
	start := strings.Index(streamARN, marker)
	if start < 0 {
		marker = "/table/"
		start = strings.Index(streamARN, marker)
	}
	if start < 0 {
		return "", fmt.Errorf("invalid stream arn")
	}
	remainder := streamARN[start+len(marker):]
	parts := strings.Split(remainder, "/")
	if len(parts) < 3 || parts[1] != "stream" {
		return "", fmt.Errorf("invalid stream arn")
	}
	if strings.TrimSpace(parts[0]) == "" {
		return "", fmt.Errorf("invalid stream arn")
	}
	return strings.TrimSpace(parts[0]), nil
}
