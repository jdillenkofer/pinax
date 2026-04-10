package lua

import (
	"context"
	"errors"
	"net"
	"net/textproto"
	"strings"

	"github.com/Shopify/go-lua"
	"github.com/jdillenkofer/pinax/internal/httpapi/authorization"
)

const authorizationFunctionName = "authorizeRequest"

var errAuthorizationFunctionNotFound = errors.New("authorization function authorizeRequest not found in Lua code")

type LuaAuthorizer struct {
	code                  string
	trustForwardedHeaders bool
	trustedProxyCIDRs     []*net.IPNet
}

type Options struct {
	TrustForwardedHeaders bool
	TrustedProxyCIDRs     []string
}

func NewLuaAuthorizer(code string) (*LuaAuthorizer, error) {
	return NewLuaAuthorizerWithOptions(code, Options{})
}

func parseTrustedProxyCIDRs(cidrStrings []string) []*net.IPNet {
	if len(cidrStrings) == 0 {
		return nil
	}
	parsed := make([]*net.IPNet, 0, len(cidrStrings))
	for _, cidrStr := range cidrStrings {
		_, ipNet, err := net.ParseCIDR(cidrStr)
		if err != nil {
			continue
		}
		parsed = append(parsed, ipNet)
	}
	return parsed
}

func NewLuaAuthorizerWithOptions(code string, options Options) (*LuaAuthorizer, error) {
	a := &LuaAuthorizer{
		code:                  code,
		trustForwardedHeaders: options.TrustForwardedHeaders,
		trustedProxyCIDRs:     parseTrustedProxyCIDRs(options.TrustedProxyCIDRs),
	}
	_, err := a.AuthorizeRequest(context.Background(), &authorization.Request{Operation: "GetItem"})
	if err != nil {
		return nil, err
	}
	return a, nil
}

func isTrustedProxy(remoteIP *string, trustedProxyCIDRs []*net.IPNet) bool {
	if remoteIP == nil {
		return false
	}
	ip := net.ParseIP(*remoteIP)
	if ip == nil {
		return false
	}
	if len(trustedProxyCIDRs) == 0 {
		return true
	}
	for _, cidr := range trustedProxyCIDRs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

func getHeaderValuesCaseInsensitive(headers map[string][]string, key string) []string {
	canonicalKey := textproto.CanonicalMIMEHeaderKey(key)
	if values, ok := headers[canonicalKey]; ok {
		return values
	}
	for headerName, values := range headers {
		if strings.EqualFold(headerName, key) {
			return values
		}
	}
	return nil
}

func getHeaderIgnoreCase(headers map[string][]string, key string) *string {
	values := getHeaderValuesCaseInsensitive(headers, key)
	if len(values) == 0 {
		return nil
	}
	v := values[0]
	return &v
}

func parseForwardedClientIP(forwardedFor string) *string {
	parts := strings.Split(forwardedFor, ",")
	if len(parts) == 0 {
		return nil
	}
	first := strings.TrimSpace(parts[0])
	ip := net.ParseIP(first)
	if ip == nil {
		return nil
	}
	parsedIP := ip.String()
	return &parsedIP
}

func parseForwardedScheme(forwardedProto string) *string {
	parts := strings.Split(forwardedProto, ",")
	if len(parts) == 0 {
		return nil
	}
	first := strings.ToLower(strings.TrimSpace(parts[0]))
	if first == "http" || first == "https" {
		return &first
	}
	return nil
}

func (a *LuaAuthorizer) resolveClientIPAndScheme(httpRequest authorization.HTTPRequest) (*string, string) {
	clientIP := httpRequest.RemoteIP
	scheme := httpRequest.Scheme
	if scheme == "" {
		scheme = "http"
	}

	if !a.trustForwardedHeaders || !isTrustedProxy(httpRequest.RemoteIP, a.trustedProxyCIDRs) {
		return clientIP, scheme
	}

	if cfConnectingIP := getHeaderIgnoreCase(httpRequest.Headers, "CF-Connecting-IP"); cfConnectingIP != nil {
		if ip := net.ParseIP(strings.TrimSpace(*cfConnectingIP)); ip != nil {
			parsedIP := ip.String()
			clientIP = &parsedIP
		}
	} else if xForwardedFor := getHeaderIgnoreCase(httpRequest.Headers, "X-Forwarded-For"); xForwardedFor != nil {
		if parsed := parseForwardedClientIP(*xForwardedFor); parsed != nil {
			clientIP = parsed
		}
	}

	if xForwardedProto := getHeaderIgnoreCase(httpRequest.Headers, "X-Forwarded-Proto"); xForwardedProto != nil {
		if parsedScheme := parseForwardedScheme(*xForwardedProto); parsedScheme != nil {
			scheme = *parsedScheme
		}
	}

	return clientIP, scheme
}

func (a *LuaAuthorizer) AuthorizeRequest(ctx context.Context, request *authorization.Request) (bool, error) {
	_ = ctx
	clientIP, scheme := a.resolveClientIPAndScheme(request.HTTPRequest)
	request.HTTPRequest.ClientIP = clientIP
	request.HTTPRequest.Scheme = scheme

	L := lua.NewState()
	lua.Require(L, "_G", lua.BaseOpen, true)
	L.Pop(1)
	lua.Require(L, "table", lua.TableOpen, true)
	L.Pop(1)
	lua.Require(L, "string", lua.StringOpen, true)
	L.Pop(1)

	if err := lua.DoString(L, a.code); err != nil {
		return false, err
	}
	L.Global(authorizationFunctionName)
	if !L.IsFunction(-1) {
		return false, errAuthorizationFunctionNotFound
	}

	a.pushRequest(L, request)
	if err := L.ProtectedCall(1, 1, 0); err != nil {
		return false, err
	}
	res := L.ToBoolean(-1)
	L.Pop(1)
	return res, nil
}

func (a *LuaAuthorizer) pushRequest(L *lua.State, request *authorization.Request) {
	L.NewTable()
	L.PushString(request.Operation)
	L.SetField(-2, "operation")

	if request.TableName != nil {
		L.PushString(*request.TableName)
	} else {
		L.PushNil()
	}
	L.SetField(-2, "tableName")

	L.NewTable()
	if request.Authorization.AccessKeyID != nil {
		L.PushString(*request.Authorization.AccessKeyID)
	} else {
		L.PushNil()
	}
	L.SetField(-2, "accessKeyId")
	L.SetField(-2, "authorization")

	L.NewTable()
	L.PushString(request.HTTPRequest.Method)
	L.SetField(-2, "method")
	L.PushString(request.HTTPRequest.Path)
	L.SetField(-2, "path")
	L.PushString(request.HTTPRequest.Query)
	L.SetField(-2, "query")
	L.PushString(request.HTTPRequest.Host)
	L.SetField(-2, "host")
	L.PushString(request.HTTPRequest.Proto)
	L.SetField(-2, "proto")
	L.PushString(request.HTTPRequest.Scheme)
	L.SetField(-2, "scheme")
	L.NewTable()
	for k, vals := range request.HTTPRequest.Headers {
		L.NewTable()
		for i, v := range vals {
			L.PushString(v)
			L.RawSetInt(-2, i+1)
		}
		L.SetField(-2, k)
	}
	L.SetField(-2, "headers")
	L.NewTable()
	for k, vals := range request.HTTPRequest.QueryParams {
		L.NewTable()
		for i, v := range vals {
			L.PushString(v)
			L.RawSetInt(-2, i+1)
		}
		L.SetField(-2, k)
	}
	L.SetField(-2, "queryParams")
	if request.HTTPRequest.ClientIP != nil {
		L.PushString(*request.HTTPRequest.ClientIP)
	} else {
		L.PushNil()
	}
	L.SetField(-2, "clientIP")
	if request.HTTPRequest.RemoteIP != nil {
		L.PushString(*request.HTTPRequest.RemoteIP)
	} else {
		L.PushNil()
	}
	L.SetField(-2, "remoteIP")
	L.SetField(-2, "httpRequest")

	L.Field(-1, "httpRequest")
	L.PushGoFunction(func(L *lua.State) int {
		expectedMethod, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(strings.EqualFold(request.HTTPRequest.Method, expectedMethod))
		return 1
	})
	L.SetField(-2, "isMethod")

	L.PushGoFunction(func(L *lua.State) int {
		headerName, ok := L.ToString(2)
		if !ok {
			L.PushNil()
			return 1
		}
		headerValues := getHeaderValuesCaseInsensitive(request.HTTPRequest.Headers, headerName)
		if len(headerValues) == 0 {
			L.PushNil()
			return 1
		}
		L.PushString(headerValues[0])
		return 1
	})
	L.SetField(-2, "header")
	L.PushGoFunction(func(L *lua.State) int {
		headerName, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		headerValues := getHeaderValuesCaseInsensitive(request.HTTPRequest.Headers, headerName)
		L.PushBoolean(len(headerValues) > 0)
		return 1
	})
	L.SetField(-2, "hasHeader")
	L.PushGoFunction(func(L *lua.State) int {
		headerName, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		expectedValue, ok := L.ToString(3)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		headerValues := getHeaderValuesCaseInsensitive(request.HTTPRequest.Headers, headerName)
		for _, headerValue := range headerValues {
			if headerValue == expectedValue {
				L.PushBoolean(true)
				return 1
			}
		}
		L.PushBoolean(false)
		return 1
	})
	L.SetField(-2, "headerEquals")
	L.PushGoFunction(func(L *lua.State) int {
		paramName, ok := L.ToString(2)
		if !ok {
			L.PushNil()
			return 1
		}
		paramValues := request.HTTPRequest.QueryParams[paramName]
		if len(paramValues) == 0 {
			L.PushNil()
			return 1
		}
		L.PushString(paramValues[0])
		return 1
	})
	L.SetField(-2, "queryParam")
	L.PushGoFunction(func(L *lua.State) int {
		paramName, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		paramValues := request.HTTPRequest.QueryParams[paramName]
		L.PushBoolean(len(paramValues) > 0)
		return 1
	})
	L.SetField(-2, "hasQueryParam")
	L.PushGoFunction(func(L *lua.State) int {
		paramName, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		expectedValue, ok := L.ToString(3)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		for _, queryParamValue := range request.HTTPRequest.QueryParams[paramName] {
			if queryParamValue == expectedValue {
				L.PushBoolean(true)
				return 1
			}
		}
		L.PushBoolean(false)
		return 1
	})
	L.SetField(-2, "queryParamEquals")
	L.PushGoFunction(func(L *lua.State) int {
		expectedPath, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(request.HTTPRequest.Path == expectedPath)
		return 1
	})
	L.SetField(-2, "pathEquals")
	L.PushGoFunction(func(L *lua.State) int {
		prefix, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(strings.HasPrefix(request.HTTPRequest.Path, prefix))
		return 1
	})
	L.SetField(-2, "pathHasPrefix")
	L.PushGoFunction(func(L *lua.State) int {
		expectedHost, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(strings.EqualFold(request.HTTPRequest.Host, expectedHost))
		return 1
	})
	L.SetField(-2, "hostEquals")
	L.PushGoFunction(func(L *lua.State) int {
		suffix, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(strings.HasSuffix(strings.ToLower(request.HTTPRequest.Host), strings.ToLower(suffix)))
		return 1
	})
	L.SetField(-2, "hostHasSuffix")
	L.PushGoFunction(func(L *lua.State) int {
		expectedScheme, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(strings.EqualFold(request.HTTPRequest.Scheme, expectedScheme))
		return 1
	})
	L.SetField(-2, "isScheme")
	L.PushGoFunction(func(L *lua.State) int {
		expectedProto, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(strings.EqualFold(request.HTTPRequest.Proto, expectedProto))
		return 1
	})
	L.SetField(-2, "isProto")
	L.PushGoFunction(func(L *lua.State) int {
		cidr, ok := L.ToString(2)
		if !ok || request.HTTPRequest.ClientIP == nil {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(ipInCIDR(*request.HTTPRequest.ClientIP, cidr))
		return 1
	})
	L.SetField(-2, "clientIPInCIDR")
	L.PushGoFunction(func(L *lua.State) int {
		cidr, ok := L.ToString(2)
		if !ok || request.HTTPRequest.RemoteIP == nil {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(ipInCIDR(*request.HTTPRequest.RemoteIP, cidr))
		return 1
	})
	L.SetField(-2, "remoteIPInCIDR")
	L.Pop(1)

	L.PushGoFunction(func(L *lua.State) int {
		L.Field(1, "authorization")
		L.Field(-1, "accessKeyId")
		L.PushBoolean(L.IsNil(-1))
		return 1
	})
	L.SetField(-2, "isAnonymous")

	L.PushGoFunction(func(L *lua.State) int {
		expectedOperation, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.Field(1, "operation")
		operation, _ := L.ToString(-1)
		L.PushBoolean(operation == expectedOperation)
		return 1
	})
	L.SetField(-2, "isOperation")

	L.PushGoFunction(func(L *lua.State) int {
		expectedOperations, ok := luaStringSliceArg(L, 2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.Field(1, "operation")
		operation, _ := L.ToString(-1)
		L.PushBoolean(stringInSlice(operation, expectedOperations))
		return 1
	})
	L.SetField(-2, "isOperationIn")

	L.PushGoFunction(func(L *lua.State) int {
		L.Field(1, "operation")
		operation, _ := L.ToString(-1)
		L.PushBoolean(isReadOnly(operation))
		return 1
	})
	L.SetField(-2, "isReadOnly")

	L.PushGoFunction(func(L *lua.State) int {
		L.Field(1, "operation")
		operation, _ := L.ToString(-1)
		L.PushBoolean(!isReadOnly(operation))
		return 1
	})
	L.SetField(-2, "isWriteOperation")

	L.PushGoFunction(func(L *lua.State) int {
		L.Field(1, "authorization")
		L.Field(-1, "accessKeyId")
		L.PushBoolean(!L.IsNil(-1))
		return 1
	})
	L.SetField(-2, "hasAccessKeyId")

	L.PushGoFunction(func(L *lua.State) int {
		expectedAccessKeyID, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.Field(1, "authorization")
		L.Field(-1, "accessKeyId")
		accessKeyID, ok := L.ToString(-1)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(accessKeyID == expectedAccessKeyID)
		return 1
	})
	L.SetField(-2, "accessKeyIdEquals")

	L.PushGoFunction(func(L *lua.State) int {
		expectedAccessKeyIDs, ok := luaStringSliceArg(L, 2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.Field(1, "authorization")
		L.Field(-1, "accessKeyId")
		accessKeyID, ok := L.ToString(-1)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(stringInSlice(accessKeyID, expectedAccessKeyIDs))
		return 1
	})
	L.SetField(-2, "accessKeyIdIn")

	L.PushGoFunction(func(L *lua.State) int {
		expectedTableName, ok := L.ToString(2)
		if !ok || request.TableName == nil {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(*request.TableName == expectedTableName)
		return 1
	})
	L.SetField(-2, "tableEquals")

	L.PushGoFunction(func(L *lua.State) int {
		prefix, ok := L.ToString(2)
		if !ok || request.TableName == nil {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(strings.HasPrefix(*request.TableName, prefix))
		return 1
	})
	L.SetField(-2, "tableHasPrefix")

	L.PushGoFunction(func(L *lua.State) int {
		expectedMethod, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.Field(1, "httpRequest")
		L.Field(-1, "method")
		method, _ := L.ToString(-1)
		L.PushBoolean(strings.EqualFold(method, expectedMethod))
		return 1
	})
	L.SetField(-2, "isMethod")
}

func stringInSlice(value string, values []string) bool {
	for _, currentValue := range values {
		if currentValue == value {
			return true
		}
	}
	return false
}

func luaStringSliceArg(L *lua.State, index int) ([]string, bool) {
	if !L.IsTable(index) {
		return nil, false
	}
	result := make([]string, 0)
	for i := 1; ; i++ {
		L.RawGetInt(index, i)
		if L.IsNil(-1) {
			L.Pop(1)
			break
		}
		value, ok := L.ToString(-1)
		L.Pop(1)
		if !ok {
			return nil, false
		}
		result = append(result, value)
	}
	return result, true
}

func ipInCIDR(ipStr string, cidr string) bool {
	ip := net.ParseIP(strings.TrimSpace(ipStr))
	if ip == nil {
		return false
	}
	_, ipNet, err := net.ParseCIDR(strings.TrimSpace(cidr))
	if err != nil {
		return false
	}
	return ipNet.Contains(ip)
}

func isReadOnly(operation string) bool {
	switch operation {
	case "DescribeTable", "DescribeLimits", "DescribeEndpoints", "ListTables", "GetItem", "Query", "Scan", "BatchGetItem", "TransactGetItems", "DescribeTimeToLive", "DescribeContinuousBackups":
		return true
	case "DescribeBackup", "ListBackups", "ListTagsOfResource":
		return true
	case "ListStreams", "DescribeStream", "GetShardIterator", "GetRecords":
		return true
	case "CreateTable", "DeleteTable", "PutItem", "DeleteItem", "UpdateItem", "BatchWriteItem", "CreateBackup", "DeleteBackup", "RestoreTableFromBackup", "UpdateContinuousBackups", "RestoreTableToPointInTime", "TagResource", "UntagResource":
		return false
	default:
		return false
	}
}
