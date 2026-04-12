package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	resourcepolicyapp "github.com/jdillenkofer/pinax/internal/app/resourcepolicy"
	tableapp "github.com/jdillenkofer/pinax/internal/app/table"
	"github.com/jdillenkofer/pinax/internal/awserr"
)

type tagResourceRequest struct {
	ResourceARN string `json:"ResourceArn"`
	Tags        []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsOfResourceRequest struct {
	ResourceARN string `json:"ResourceArn"`
	NextToken   string `json:"NextToken"`
}

func (s *Server) tagResource(r *http.Request, body []byte) (map[string]any, error) {
	var req tagResourceRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.ResourceARN) == "" {
		return nil, awserr.Validation("ResourceArn is required")
	}
	if len(req.Tags) == 0 {
		return nil, awserr.Validation("Tags is required")
	}
	tags, err := normalizeTags(req.Tags)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}

	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, &awserr.APIError{Code: "AccessDeniedException", Message: "Access denied", Status: http.StatusBadRequest}
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	if err := s.tableService.TagTable(r.Context(), tableKey, tags, lifecycleNow()); err != nil {
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return nil, awserr.ResourceNotFound("Requested resource not found")
		}
		if errors.Is(err, tableapp.ErrTooManyTags) {
			return nil, awserr.Validation("Tags can have at most 50 items")
		}
		return nil, err
	}
	return map[string]any{}, nil
}

func (s *Server) untagResource(r *http.Request, body []byte) (map[string]any, error) {
	var req untagResourceRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.ResourceARN) == "" {
		return nil, awserr.Validation("ResourceArn is required")
	}
	if len(req.TagKeys) == 0 {
		return nil, awserr.Validation("TagKeys is required")
	}
	keys, err := normalizeTagKeys(req.TagKeys)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}

	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, &awserr.APIError{Code: "AccessDeniedException", Message: "Access denied", Status: http.StatusBadRequest}
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	if err := s.tableService.UntagTable(r.Context(), tableKey, keys, lifecycleNow()); err != nil {
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return nil, awserr.ResourceNotFound("Requested resource not found")
		}
		return nil, err
	}
	return map[string]any{}, nil
}

func (s *Server) listTagsOfResource(r *http.Request, body []byte) (map[string]any, error) {
	var req listTagsOfResourceRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.ResourceARN) == "" {
		return nil, awserr.Validation("ResourceArn is required")
	}

	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, &awserr.APIError{Code: "AccessDeniedException", Message: "Access denied", Status: http.StatusBadRequest}
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	sortedTags, err := s.tableService.ListTableTags(r.Context(), tableKey, lifecycleNow())
	if err != nil {
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return nil, awserr.ResourceNotFound("Requested resource not found")
		}
		return nil, err
	}
	tags := make([]map[string]any, 0, len(sortedTags))
	start, err := parseListTagsOfResourceStartToken(req.NextToken, len(sortedTags))
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	end := len(sortedTags)
	if max := start + listTagsOfResourcePageSize; max < end {
		end = max
	}
	for _, tag := range sortedTags[start:end] {
		tags = append(tags, map[string]any{"Key": tag.Key, "Value": tag.Value})
	}
	resp := map[string]any{"Tags": tags}
	if end < len(sortedTags) {
		resp["NextToken"] = encodeListTagsOfResourceStartToken(end)
	}
	return resp, nil
}

func parseListTagsOfResourceStartToken(nextToken string, total int) (int, error) {
	token := strings.TrimSpace(nextToken)
	if token == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("Invalid NextToken")
	}
	start, err := strconv.Atoi(string(raw))
	if err != nil || start < 0 || start >= total {
		return 0, fmt.Errorf("Invalid NextToken")
	}
	return start, nil
}

func encodeListTagsOfResourceStartToken(start int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(start)))
}

type putResourcePolicyRequest struct {
	ResourceARN                     string `json:"ResourceArn"`
	Policy                          string `json:"Policy"`
	ExpectedRevisionID              string `json:"ExpectedRevisionId"`
	ConfirmRemoveSelfResourceAccess bool   `json:"ConfirmRemoveSelfResourceAccess"`
}

type getResourcePolicyRequest struct {
	ResourceARN string `json:"ResourceArn"`
}

type deleteResourcePolicyRequest struct {
	ResourceARN        string `json:"ResourceArn"`
	ExpectedRevisionID string `json:"ExpectedRevisionId"`
}

func (s *Server) putResourcePolicy(r *http.Request, body []byte) (map[string]any, error) {
	var req putResourcePolicyRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	resourceARN, isStream, err := validateResourcePolicyARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if err := validateResourcePolicyDocument(req.Policy); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if err := validateExpectedRevisionID(req.ExpectedRevisionID); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(resourceARN)
	if err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	if err := s.resourcePolicyService.EnsureTarget(r.Context(), tableKey, resourceARN, isStream, lifecycleNow()); err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}

	revisionID, err := s.resourcePolicyService.Put(r.Context(), resourceARN, req.Policy, req.ExpectedRevisionID, req.ConfirmRemoveSelfResourceAccess, needsConfirmRemoveSelfResourceAccess)
	if err != nil {
		if errors.Is(err, resourcepolicyapp.ErrPolicyNotFound) {
			return nil, awserr.PolicyNotFound(policyNotFoundMessage)
		}
		if errors.Is(err, resourcepolicyapp.ErrConfirmRemoveSelfResourceAccessRequired) {
			return nil, awserr.Validation("This policy contains a statement that may prevent future policy updates for this resource. Set ConfirmRemoveSelfResourceAccess to true to confirm this change")
		}
		return nil, err
	}
	return map[string]any{"RevisionId": revisionID}, nil
}

func (s *Server) getResourcePolicy(r *http.Request, body []byte) (map[string]any, error) {
	var req getResourcePolicyRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	resourceARN, isStream, err := validateResourcePolicyARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}

	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(resourceARN)
	if err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	if err := s.resourcePolicyService.EnsureTarget(r.Context(), tableKey, resourceARN, isStream, lifecycleNow()); err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	policy, revisionID, err := s.resourcePolicyService.Get(r.Context(), resourceARN)
	if err != nil {
		if errors.Is(err, resourcepolicyapp.ErrPolicyNotFound) {
			return nil, awserr.PolicyNotFound(policyNotFoundMessage)
		}
		return nil, err
	}
	return map[string]any{"Policy": policy, "RevisionId": revisionID}, nil
}

func (s *Server) deleteResourcePolicy(r *http.Request, body []byte) (map[string]any, error) {
	var req deleteResourcePolicyRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	resourceARN, isStream, err := validateResourcePolicyARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if err := validateExpectedRevisionID(req.ExpectedRevisionID); err != nil {
		return nil, awserr.Validation(err.Error())
	}

	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(resourceARN)
	if err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	if err := s.resourcePolicyService.EnsureTarget(r.Context(), tableKey, resourceARN, isStream, lifecycleNow()); err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	revisionID, err := s.resourcePolicyService.Delete(r.Context(), resourceARN, req.ExpectedRevisionID)
	if err != nil {
		if errors.Is(err, resourcepolicyapp.ErrPolicyNotFound) {
			return nil, awserr.PolicyNotFound(policyNotFoundMessage)
		}
		return nil, err
	}
	return map[string]any{"RevisionId": revisionID}, nil
}

func validateResourcePolicyARN(resourceARN string) (string, bool, error) {
	resourceARN = strings.TrimSpace(resourceARN)
	if resourceARN == "" {
		return "", false, fmt.Errorf("ResourceArn is required")
	}
	if len(resourceARN) > 1283 {
		return "", false, fmt.Errorf("Member must have length less than or equal to 1283")
	}
	if !strings.HasPrefix(resourceARN, "arn:") {
		return "", false, fmt.Errorf("ResourceArn must be a valid ARN")
	}
	tableName, _, err := taggableTableNameFromResourceARN(resourceARN)
	if err != nil || strings.TrimSpace(tableName) == "" {
		return "", false, fmt.Errorf("ResourceArn must identify a DynamoDB table or stream")
	}
	marker := ":table/" + tableName
	start := strings.Index(resourceARN, marker)
	if start < 0 {
		marker = "/table/" + tableName
		start = strings.Index(resourceARN, marker)
	}
	if start < 0 {
		return "", false, fmt.Errorf("ResourceArn must identify a DynamoDB table or stream")
	}
	remainder := resourceARN[start+len(marker):]
	if remainder == "" {
		return resourceARN, false, nil
	}
	if !strings.HasPrefix(remainder, "/stream/") {
		return "", false, fmt.Errorf("ResourceArn must identify a DynamoDB table or stream")
	}
	streamLabel := strings.TrimSpace(strings.TrimPrefix(remainder, "/stream/"))
	if streamLabel == "" || strings.Contains(streamLabel, "/") {
		return "", false, fmt.Errorf("ResourceArn must identify a DynamoDB table or stream")
	}
	return resourceARN, true, nil
}

func validateResourcePolicyDocument(policy string) error {
	if strings.TrimSpace(policy) == "" {
		return fmt.Errorf("Policy is required")
	}
	if len(policy) > maxResourcePolicyBytes {
		return fmt.Errorf("Policy must be less than or equal to 20480 bytes")
	}
	if !json.Valid([]byte(policy)) {
		return fmt.Errorf("Policy must be valid JSON")
	}
	return nil
}

func validateExpectedRevisionID(expected string) error {
	expected = strings.TrimSpace(expected)
	if expected == "" {
		return nil
	}
	if len(expected) > 255 {
		return fmt.Errorf("Member must have length less than or equal to 255")
	}
	return nil
}

func needsConfirmRemoveSelfResourceAccess(resourceARN, policy string) bool {
	var doc map[string]any
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		return false
	}
	selfPrincipals := []string{}
	if root := rootPrincipalFromResourceARN(resourceARN); root != "" {
		selfPrincipals = append(selfPrincipals, root)
	}
	statements := normalizePolicyStatements(doc["Statement"])
	for _, raw := range statements {
		stmt, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(valueAsString(stmt["Effect"])), "Deny") {
			continue
		}
		if !policyStatementTargetsSelfResource(stmt, resourceARN) {
			continue
		}
		if !policyStatementDeniesPolicyMutation(stmt) {
			continue
		}
		if policyStatementCanMatchSelfPrincipal(stmt, selfPrincipals) {
			return true
		}
	}
	return false
}

func normalizePolicyStatements(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	case map[string]any:
		return []any{t}
	default:
		return nil
	}
}

func policyStatementDeniesPolicyMutation(stmt map[string]any) bool {
	if notAction, ok := stmt["NotAction"]; ok {
		notActions := normalizedActionSet(notAction)
		if !containsAction(notActions, "dynamodb:PutResourcePolicy") || !containsAction(notActions, "dynamodb:DeleteResourcePolicy") {
			return true
		}
		return false
	}
	actions := normalizedActionSet(stmt["Action"])
	for _, a := range actions {
		a = strings.ToLower(strings.TrimSpace(a))
		if a == "*" || a == "dynamodb:*" || a == "dynamodb:putresourcepolicy" || a == "dynamodb:deleteresourcepolicy" {
			return true
		}
	}
	return false
}

func policyStatementTargetsSelfResource(stmt map[string]any, targetARN string) bool {
	if notResource, ok := stmt["NotResource"]; ok {
		notResources := normalizedResourceSet(notResource)
		for _, nr := range notResources {
			nr = strings.TrimSpace(nr)
			if nr == "*" || nr == targetARN {
				return false
			}
		}
		return true
	}
	resources := normalizedResourceSet(stmt["Resource"])
	for _, r := range resources {
		r = strings.TrimSpace(r)
		if r == "*" || r == targetARN {
			return true
		}
	}
	return false
}

func policyStatementCanMatchSelfPrincipal(stmt map[string]any, selfPrincipals []string) bool {
	normalizedSelf := make([]string, 0, len(selfPrincipals))
	for _, s := range selfPrincipals {
		normalizedSelf = append(normalizedSelf, strings.ToLower(strings.TrimSpace(s)))
	}
	if notPrincipal, ok := stmt["NotPrincipal"]; ok {
		excluded := normalizedPrincipalSet(notPrincipal)
		for _, self := range normalizedSelf {
			if !containsPrincipal(excluded, self) && !containsPrincipal(excluded, "*") {
				return true
			}
		}
		return false
	}
	principal, ok := stmt["Principal"]
	if !ok {
		return true
	}
	allowed := normalizedPrincipalSet(principal)
	for _, self := range normalizedSelf {
		if containsPrincipal(allowed, self) || containsPrincipal(allowed, "*") {
			return true
		}
	}
	return false
}

func normalizedActionSet(v any) []string {
	return stringSetFromPolicyField(v)
}

func normalizedResourceSet(v any) []string {
	return stringSetFromPolicyField(v)
}

func normalizedPrincipalSet(principal any) []string {
	switch p := principal.(type) {
	case string:
		return []string{strings.ToLower(strings.TrimSpace(p))}
	case map[string]any:
		out := make([]string, 0)
		for k, vv := range p {
			if !strings.EqualFold(strings.TrimSpace(k), "AWS") {
				continue
			}
			for _, candidate := range stringSetFromPolicyField(vv) {
				out = append(out, strings.ToLower(strings.TrimSpace(candidate)))
			}
		}
		return out
	}
	return nil
}

func rootPrincipalFromResourceARN(resourceARN string) string {
	parts := strings.Split(resourceARN, ":")
	if len(parts) < 6 {
		return ""
	}
	acct := strings.TrimSpace(parts[4])
	if acct == "" {
		return ""
	}
	return "arn:aws:iam::" + acct + ":root"
}

func stringSetFromPolicyField(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func containsAction(actions []string, action string) bool {
	action = strings.ToLower(strings.TrimSpace(action))
	for _, a := range actions {
		a = strings.ToLower(strings.TrimSpace(a))
		if a == action || a == "*" || a == "dynamodb:*" {
			return true
		}
	}
	return false
}

func containsPrincipal(principals []string, principal string) bool {
	principal = strings.ToLower(strings.TrimSpace(principal))
	for _, p := range principals {
		if strings.ToLower(strings.TrimSpace(p)) == principal {
			return true
		}
	}
	return false
}

func valueAsString(v any) string {
	s, _ := v.(string)
	return s
}
