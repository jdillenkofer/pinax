package authentication

import (
	"bytes"
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

const contentSHA256Header = "x-amz-content-sha256"
const contentSHA256UnsignedPayload = "UNSIGNED-PAYLOAD"

const signatureAlgorithm = "AWS4-HMAC-SHA256"
const expectedRequest = "aws4_request"

var allowedServices = map[string]struct{}{
	"dynamodb":        {},
	"dynamodbstreams": {},
}

type AccessKeyIDContextKey struct{}
type AuthTypeContextKey struct{}
type RequestIDContextKey struct{}
type ClientIPContextKey struct{}
type IsAuthenticatedContextKey struct{}

type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

func hmacSha256(secret []byte, data []byte) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write(data)
	return h.Sum(nil)
}

func createSigningKey(secretAccessKey string, date string, region string, service string, request string) []byte {
	dateKey := hmacSha256([]byte("AWS4"+secretAccessKey), []byte(date))
	dateRegionKey := hmacSha256(dateKey, []byte(region))
	dateRegionServiceKey := hmacSha256(dateRegionKey, []byte(service))
	return hmacSha256(dateRegionServiceKey, []byte(request))
}

func createSignature(signingKey []byte, stringToSign string) string {
	data := hmacSha256(signingKey, []byte(stringToSign))
	return hex.EncodeToString(data)
}

type pair struct {
	key string
	val string
}

func uriEncode(input string) string {
	output := url.QueryEscape(input)
	output = strings.ReplaceAll(output, "+", "%20")
	output = strings.ReplaceAll(output, "*", "%2A")
	output = strings.ReplaceAll(output, "%7E", "~")
	return output
}

func generateCanonicalURI(r *http.Request) string {
	escapedPath := r.URL.EscapedPath()
	if escapedPath == "" {
		return "/"
	}
	return escapedPath
}

func generateCanonicalQueryString(r *http.Request) string {
	queryStrings := []pair{}
	for queryKey, queryValues := range r.URL.Query() {
		if queryKey == "X-Amz-Signature" {
			continue
		}
		encodedQueryKey := uriEncode(queryKey)
		for _, queryVal := range queryValues {
			encodedQueryVal := uriEncode(queryVal)
			queryStrings = append(queryStrings, pair{key: encodedQueryKey, val: encodedQueryVal})
		}
	}
	slices.SortFunc(queryStrings, func(a, b pair) int {
		byKey := cmp.Compare(a.key, b.key)
		if byKey != 0 {
			return byKey
		}
		return cmp.Compare(a.val, b.val)
	})

	canonicalQueryString := ""
	for idx, queryStringPair := range queryStrings {
		canonicalQueryString += queryStringPair.key + "=" + queryStringPair.val
		if idx < len(queryStrings)-1 {
			canonicalQueryString += "&"
		}
	}
	return canonicalQueryString
}

func generateCanonicalHeaders(r *http.Request, headersToInclude []string) string {
	headers := []pair{{key: "host", val: strings.TrimSpace(r.Host)}}
	for headerKey, headerValues := range r.Header {
		headerKey = strings.ToLower(headerKey)
		if slices.Contains(headersToInclude, headerKey) {
			headerVal := strings.TrimSpace(strings.Join(headerValues, ","))
			headers = append(headers, pair{key: headerKey, val: headerVal})
		}
	}
	slices.SortFunc(headers, func(a, b pair) int { return cmp.Compare(a.key, b.key) })

	canonicalHeaders := ""
	for _, header := range headers {
		canonicalHeaders += header.key + ":" + header.val + "\n"
	}
	return canonicalHeaders
}

func generateSignedHeaders(r *http.Request, headersToInclude []string) string {
	headers := []pair{{key: "host", val: strings.TrimSpace(r.Host)}}
	for headerKey, headerValues := range r.Header {
		headerKey = strings.ToLower(headerKey)
		if slices.Contains(headersToInclude, headerKey) {
			headerVal := strings.TrimSpace(strings.Join(headerValues, ","))
			headers = append(headers, pair{key: headerKey, val: headerVal})
		}
	}
	slices.SortFunc(headers, func(a, b pair) int { return cmp.Compare(a.key, b.key) })

	signedHeaders := ""
	for idx, header := range headers {
		signedHeaders += header.key
		if idx < len(headers)-1 {
			signedHeaders += ";"
		}
	}
	return signedHeaders
}

func generateHashedPayload(r *http.Request) (*string, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	sha256Hash := sha256.Sum256(body)
	hexSha256 := hex.EncodeToString(sha256Hash[:])
	return &hexSha256, nil
}

func generateCanonicalRequest(r *http.Request, headersToInclude []string, isPresigned bool) (*string, error) {
	canonicalRequest := r.Method + "\n"
	canonicalRequest += generateCanonicalURI(r) + "\n"
	canonicalRequest += generateCanonicalQueryString(r) + "\n"
	canonicalRequest += generateCanonicalHeaders(r, headersToInclude) + "\n"
	canonicalRequest += generateSignedHeaders(r, headersToInclude) + "\n"

	contentSHA256 := r.Header.Get(contentSHA256Header)
	if isPresigned || contentSHA256 == contentSHA256UnsignedPayload {
		canonicalRequest += contentSHA256UnsignedPayload
	} else {
		hashedPayload, err := generateHashedPayload(r)
		if err != nil {
			return nil, err
		}
		canonicalRequest += *hashedPayload
	}
	return &canonicalRequest, nil
}

func generateStringToSign(r *http.Request, timestamp string, scope string, headersToInclude []string, isPresigned bool) (*string, error) {
	canonicalRequest, err := generateCanonicalRequest(r, headersToInclude, isPresigned)
	if err != nil {
		return nil, err
	}
	sha := sha256.Sum256([]byte(*canonicalRequest))
	canonicalRequestHexSha256 := hex.EncodeToString(sha[:])
	stringToSign := signatureAlgorithm + "\n" + timestamp + "\n" + scope + "\n" + canonicalRequestHexSha256
	return &stringToSign, nil
}

func createScope(date string, region string, service string, request string) string {
	return date + "/" + region + "/" + service + "/" + request
}

func mustBeSignedHeader(headerKey string) bool {
	if headerKey == "content-md5" {
		return true
	}
	if strings.HasPrefix(headerKey, "x-amz-") {
		return true
	}
	return false
}

func checkAuthentication(validCredentials []Credentials, expectedRegion string, r *http.Request) (usedAccessKeyID *string, authenticated bool) {
	now := time.Now().UTC()
	expectedDate := now.Format("20060102")

	var credential string
	var timestamp string
	var expirationDuration time.Duration
	var signedHeaders string
	var signature string
	var isPresigned bool

	authorizationHeader := r.Header.Get("Authorization")
	if authorizationHeader == "" {
		isPresigned = true
		query := r.URL.Query()
		credential = query.Get("X-Amz-Credential")
		timestamp = query.Get("X-Amz-Date")
		expires := query.Get("X-Amz-Expires")
		parsedExpired, err := strconv.ParseInt(expires, 10, 32)
		if err != nil || parsedExpired < 1 || parsedExpired > 604800 {
			return nil, false
		}
		expirationDuration = time.Duration(parsedExpired) * time.Second
		signedHeaders = query.Get("X-Amz-SignedHeaders")
		signature = query.Get("X-Amz-Signature")
	} else {
		isPresigned = false
		authorizationHeader, found := strings.CutPrefix(authorizationHeader, signatureAlgorithm)
		if !found {
			return nil, false
		}
		authFields := strings.Split(authorizationHeader, ",")
		if len(authFields) != 3 {
			return nil, false
		}

		credential = strings.TrimSpace(authFields[0])
		credential, found = strings.CutPrefix(credential, "Credential=")
		if !found {
			return nil, false
		}

		timestamp = r.Header.Get("x-amz-date")
		if timestamp == "" {
			timestamp = r.Header.Get("Date")
		}
		expirationDuration = 5 * time.Minute

		signedHeaders = strings.TrimSpace(authFields[1])
		signedHeaders, found = strings.CutPrefix(signedHeaders, "SignedHeaders=")
		if !found {
			return nil, false
		}

		signature = strings.TrimSpace(authFields[2])
		signature, found = strings.CutPrefix(signature, "Signature=")
		if !found {
			return nil, false
		}
	}

	accessKeyIDAndScope := strings.Split(credential, "/")
	if len(accessKeyIDAndScope) != 5 {
		return nil, false
	}
	accessKeyID := accessKeyIDAndScope[0]
	foundIndex := slices.IndexFunc(validCredentials, func(c Credentials) bool {
		return c.AccessKeyID == accessKeyID
	})
	if foundIndex < 0 {
		return nil, false
	}
	expectedCredentials := validCredentials[foundIndex]
	date := accessKeyIDAndScope[1]
	if date != expectedDate {
		return nil, false
	}
	region := accessKeyIDAndScope[2]
	if region != expectedRegion {
		return nil, false
	}
	service := accessKeyIDAndScope[3]
	if _, ok := allowedServices[service]; !ok {
		return nil, false
	}
	if accessKeyIDAndScope[4] != expectedRequest {
		return nil, false
	}

	scope := createScope(expectedDate, region, service, expectedRequest)

	parsedTimestamp, err := time.Parse("20060102T150405Z", timestamp)
	if err != nil {
		return nil, false
	}
	beforeTimestamp := parsedTimestamp.Add(-15 * time.Minute)
	expiredTimestamp := parsedTimestamp.Add(expirationDuration)
	if now.Before(beforeTimestamp) || now.After(expiredTimestamp) {
		return nil, false
	}

	rawSignedHeadersArray := strings.Split(signedHeaders, ";")
	signedHeadersArray := make([]string, 0, len(rawSignedHeadersArray))
	for _, signedHeader := range rawSignedHeadersArray {
		signedHeader = strings.ToLower(strings.TrimSpace(signedHeader))
		if signedHeader != "" {
			signedHeadersArray = append(signedHeadersArray, signedHeader)
		}
	}
	if !slices.Contains(signedHeadersArray, "host") {
		return nil, false
	}
	for headerKey := range r.Header {
		headerKey = strings.ToLower(headerKey)
		if mustBeSignedHeader(headerKey) && !slices.Contains(signedHeadersArray, headerKey) {
			return nil, false
		}
	}

	stringToSign, err := generateStringToSign(r, timestamp, scope, signedHeadersArray, isPresigned)
	if err != nil {
		return nil, false
	}
	signingKey := createSigningKey(expectedCredentials.SecretAccessKey, expectedDate, region, service, expectedRequest)
	calculatedSignature := createSignature(signingKey, *stringToSign)
	isSignatureValid := subtle.ConstantTimeCompare([]byte(signature), []byte(calculatedSignature)) == 1
	if !isSignatureValid {
		return nil, false
	}

	return &accessKeyID, true
}

func authTypeForRequest(r *http.Request) string {
	if isAnonymousRequest(r) {
		return "anonymous"
	}
	if r.Header.Get("Authorization") != "" {
		return "sigv4-header"
	}
	if r.URL.Query().Get("X-Amz-Credential") != "" {
		return "sigv4-presign"
	}
	return "anonymous"
}

func isAnonymousRequest(r *http.Request) bool {
	if r.Header.Get("Authorization") != "" {
		return false
	}
	if r.URL.Query().Get("X-Amz-Credential") != "" {
		return false
	}
	return true
}

func MakeSignatureMiddleware(validCredentials []Credentials, region string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAnonymousRequest(r) {
			ctx := context.WithValue(r.Context(), IsAuthenticatedContextKey{}, false)
			ctx = context.WithValue(ctx, AuthTypeContextKey{}, authTypeForRequest(r))
			next.ServeHTTP(w, r.Clone(ctx))
			return
		}

		usedAccessKeyID, isAuthenticated := checkAuthentication(validCredentials, region, r)
		if isAuthenticated {
			ctx := context.WithValue(r.Context(), AccessKeyIDContextKey{}, *usedAccessKeyID)
			ctx = context.WithValue(ctx, IsAuthenticatedContextKey{}, true)
			ctx = context.WithValue(ctx, AuthTypeContextKey{}, authTypeForRequest(r))
			next.ServeHTTP(w, r.Clone(ctx))
			return
		}

		w.WriteHeader(http.StatusUnauthorized)
	})
}
