/*
 * Copyright 2017-2025 Hypermode Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package alpha

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
	jsonpb "google.golang.org/protobuf/encoding/protojson"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/hypermodeinc/dgraph/v24/dql"
	"github.com/hypermodeinc/dgraph/v24/edgraph"
	"github.com/hypermodeinc/dgraph/v24/graphql/admin"
	"github.com/hypermodeinc/dgraph/v24/graphql/schema"
	"github.com/hypermodeinc/dgraph/v24/query"
	"github.com/hypermodeinc/dgraph/v24/x"
)

func allowed(method string) bool {
	return method == http.MethodPost || method == http.MethodPut
}

// Common functionality for these request handlers. Returns true if the request is completely
// handled here and nothing further needs to be done.
// 请求处理程序的通用功能。如果请求在此处完全处理完毕，无需进一步操作，则返回true。
func commonHandler(w http.ResponseWriter, r *http.Request) bool {
	// Do these requests really need CORS headers? Doesn't seem like it, but they are probably
	// harmless aside from the extra size they add to each response.
	x.AddCorsHeaders(w)
	w.Header().Set("Content-Type", "application/json")

	if r.Method == "OPTIONS" {
		return true
	} else if !allowed(r.Method) {
		w.WriteHeader(http.StatusBadRequest)
		x.SetStatus(w, x.ErrorInvalidMethod, "Invalid method")
		return true
	}

	return false
}

// Read request body, transparently decompressing if necessary. Return nil on error.
// 读取请求正文，必要时透明解压缩。出错时返回nil。
func readRequest(w http.ResponseWriter, r *http.Request) []byte {
	var in io.Reader = r.Body

	if enc := r.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		if enc == "gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				x.SetStatus(w, x.Error, "Unable to create decompressor")
				return nil
			}
			defer gz.Close()
			in = gz
		} else {
			x.SetStatus(w, x.ErrorInvalidRequest, "Unsupported content encoding")
			return nil
		}
	}

	body, err := io.ReadAll(in)
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return nil
	}

	return body
}

// parseUint64 reads the value for given URL parameter from request and
// parses it into uint64, empty string is converted into zero value
func parseUint64(r *http.Request, name string) (uint64, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return 0, nil
	}

	uintVal, err := strconv.ParseUint(value, 0, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "while parsing %s as uint64", name)
	}

	return uintVal, nil
}

// parseBool reads the value for given URL parameter from request and
// parses it into bool, empty string is converted into zero value
// parseBool从请求中读取给定URL参数的值，并将其解析为bool，空字符串转换为零值
func parseBool(r *http.Request, name string) (bool, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return false, nil
	}

	boolval, err := strconv.ParseBool(value)
	if err != nil {
		return false, errors.Wrapf(err, "while parsing %s as bool", name)
	}

	return boolval, nil
}

// parseDuration reads the value for given URL parameter from request and
// parses it into time.Duration, empty string is converted into zero value
// parseDuration从请求中读取给定URL参数的值，并将其解析为时间。持续时间，空字符串转换为零值
func parseDuration(r *http.Request, name string) (time.Duration, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return 0, nil
	}

	durationValue, err := time.ParseDuration(value)
	if err != nil {
		return 0, errors.Wrapf(err, "while parsing %s as time.Duration", name)
	}

	return durationValue, nil
}

// This method should just build the request and proxy it to the Query method of dgraph.Server.
// It can then encode the response as appropriate before sending it back to the user.
// 此方法只需构建请求并将其代理到dgraph的Query方法。服务器。
// 然后，它可以在将响应发送回用户之前对其进行适当的编码。
func queryHandler(w http.ResponseWriter, r *http.Request) {
	// 注意这个是触发处理函数
	if commonHandler(w, r) {
		return
	}

	isDebugMode, err := parseBool(r, "debug")
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	queryTimeout, err := parseDuration(r, "timeout") //获取超时时间
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	startTs, err := parseUint64(r, "startTs")
	hash := r.URL.Query().Get("hash") // 也是获取URL中的查询参数
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}

	body := readRequest(w, r) //解析出来请求的body，此时是JSON 格式的字节切片
	if body == nil {
		return
	}

	var params struct {
		Query     string            `json:"query"`   // 查询体
		Variables map[string]string `json:"variables"`
	}

	contentType := r.Header.Get("Content-Type")
	mediaType, contentTypeParams, err := mime.ParseMediaType(contentType) // 解析请求头的Content-Type，判断出来是什么类型的请求（JSON或Graphql+-或DQL）
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, "Invalid Content-Type")
	}
	if charset, ok := contentTypeParams["charset"]; ok && strings.ToLower(charset) != "utf-8" { //判断请求的编码格式是utf-8，否则返回不支持
		x.SetStatus(w, x.ErrorInvalidRequest, "Unsupported charset. "+
			"Supported charset is UTF-8")
		return
	}

	// 下面是根据请求的格式填充params对象
	switch mediaType {
	case "application/json":
		if err := json.Unmarshal(body, &params); err != nil {
			// `json.Marshal` 用于将 Go 对象转换为 JSON 格式的字节切片。
			// `json.Unmarshal` 用于将 第一个参数的JSON 格式的字节切片解码为对应的 Go 对象，放在第二个参数params里。
			jsonErr := convertJSONError(string(body), err)
			x.SetStatus(w, x.ErrorInvalidRequest, jsonErr.Error())
			return
		}
	case "application/graphql+-", "application/dql":
		params.Query = string(body)  //直接转为字符串然后放进去就行
	default:
		x.SetStatus(w, x.ErrorInvalidRequest, "Unsupported Content-Type. "+
			"Supported content types are application/json, application/graphql+-,application/dql")
		return
	}

	ctx := context.WithValue(r.Context(), query.DebugKey, isDebugMode)  // 在请求上下文中加入 是否是debug 的标识
	ctx = x.AttachAccessJwt(ctx, r) // 在gRPC的请求上下文的metadata中加入JWT
	ctx = x.AttachRemoteIP(ctx, r) // 在gRPC请求上下文的peer中加入解析出来的发出请求的主机的IP与Port

	if queryTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, queryTimeout)  //在上下文中设置超时时间
		defer cancel()
	}

	req := api.Request{ //构建一个新的，内部用的请求对象
		Vars:    params.Variables,
		Query:   params.Query,
		StartTs: startTs,
		Hash:    hash,
	}

	if req.StartTs == 0 { // 如果还未分配开始时间戳
		// If be is set, run this as a best-effort query.
		// 如果设置了be(BEST EFFORT)，则将其作为尽力而为的查询运行。
		isBestEffort, err := parseBool(r, "be")
		if err != nil {
			x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
			return
		}
		if isBestEffort { //设置一些标志 NOTE:20254280
			req.BestEffort = true
			req.ReadOnly = true
		}

		// If ro is set, run this as a readonly query.
		// 如果设置了ro(READ ONLY)，请将其作为只读查询运行。
		isReadOnly, err := parseBool(r, "ro")
		if err != nil {
			x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
			return
		}
		if isReadOnly {
			req.ReadOnly = true
		}
	}

	// If rdf is set true, then response will be in rdf format.
	// 如果rdf设置为true，则响应将采用rdf格式。
	respFormat := r.URL.Query().Get("respFormat")
	switch respFormat {
	case "", "json":
		req.RespFormat = api.Request_JSON // 设置为 int 0
	case "rdf":
		req.RespFormat = api.Request_RDF //设置为 int 1
	default:
		x.SetStatus(w, x.ErrorInvalidRequest, fmt.Sprintf("invalid value [%v] for parameter respFormat", respFormat))
		return
	}

	// Core processing happens here.核心处理发生在这里。
	resp, err := (&edgraph.Server{}).QueryNoGrpc(ctx, &req) // NOTE:核心操作，处理查询以及突变,返回的resp里面已经会包含结果！（依靠前面生成的当前请求的上下文对象ctx以及req对象）
	// zzlTODO:下面是对返回的信息再进一步填充，暂时不看
	if err != nil {
		x.SetStatusWithData(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	// Add cost to the header.
	w.Header().Set(x.DgraphCostHeader, fmt.Sprint(resp.Metrics.NumUids["_total"]))

	e := query.Extensions{
		Txn:     resp.Txn,
		Latency: resp.Latency,
		Metrics: resp.Metrics,
	}
	js, err := json.Marshal(e)
	if err != nil {
		x.SetStatusWithData(w, x.Error, err.Error())
		return
	}

	var out bytes.Buffer
	writeEntry := func(key string, js []byte) {
		x.Check2(out.WriteRune('"'))
		x.Check2(out.WriteString(key))
		x.Check2(out.WriteRune('"'))
		x.Check2(out.WriteRune(':'))
		x.Check2(out.Write(js))
	}
	x.Check2(out.WriteRune('{'))
	if respFormat == "rdf" {
		// In Json, []byte marshals into a base64 data. We instead Marshal it as a string.
		// json.Marshal is therefore necessary here. We also do not want to escape <,>.
		var buf bytes.Buffer
		encoder := json.NewEncoder(&buf)
		encoder.SetEscapeHTML(false)
		x.Check(encoder.Encode(string(resp.Rdf)))
		writeEntry("data", buf.Bytes())
	} else {
		writeEntry("data", resp.Json)
	}
	x.Check2(out.WriteRune(','))
	writeEntry("extensions", js)
	x.Check2(out.WriteRune('}'))

	if _, err := x.WriteResponse(w, r, out.Bytes()); err != nil {
		// If client crashes before server could write response, writeResponse will error out,
		// Check2 will fatal and shut the server down in such scenario. We don't want that.
		glog.Errorln("Unable to write response: ", err)
	}
}

func mutationHandler(w http.ResponseWriter, r *http.Request) {
	if commonHandler(w, r) {
		return
	}

	commitNow, err := parseBool(r, "commitNow")
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	startTs, err := parseUint64(r, "startTs")
	hash := r.URL.Query().Get("hash")
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	body := readRequest(w, r)
	if body == nil {
		return
	}

	// start parsing the query
	parseStart := time.Now()

	var req *api.Request
	contentType := r.Header.Get("Content-Type")
	mediaType, contentTypeParams, err := mime.ParseMediaType(contentType)
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, "Invalid Content-Type")
	}
	if charset, ok := contentTypeParams["charset"]; ok && strings.ToLower(charset) != "utf-8" {
		x.SetStatus(w, x.ErrorInvalidRequest, "Unsupported charset. "+
			"Supported charset is UTF-8")
		return
	}

	switch mediaType {
	case "application/json":
		ms := make(map[string]*skipJSONUnmarshal)
		if err := json.Unmarshal(body, &ms); err != nil {
			jsonErr := convertJSONError(string(body), err)
			x.SetStatus(w, x.ErrorInvalidRequest, jsonErr.Error())
			return
		}

		req = &api.Request{}
		if queryText, ok := ms["query"]; ok && queryText != nil {
			req.Query, err = strconv.Unquote(string(queryText.bs))
			if err != nil {
				x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
				return
			}
		}

		// JSON API support both keys 1. mutations  2. set,delete,cond
		// We want to maintain the backward compatibility of the API here.
		extractMutation := func(jsMap map[string]*skipJSONUnmarshal) (*api.Mutation, error) {
			mu := &api.Mutation{}
			empty := true
			if setJSON, ok := jsMap["set"]; ok && setJSON != nil {
				empty = false
				mu.SetJson = setJSON.bs
			}
			if delJSON, ok := jsMap["delete"]; ok && delJSON != nil {
				empty = false
				mu.DeleteJson = delJSON.bs
			}
			if condText, ok := jsMap["cond"]; ok && condText != nil {
				mu.Cond, err = strconv.Unquote(string(condText.bs))
				if err != nil {
					return nil, err
				}
			}

			if empty {
				return nil, nil
			}

			return mu, nil
		}
		if mu, err := extractMutation(ms); err != nil {
			x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
			return
		} else if mu != nil {
			req.Mutations = append(req.Mutations, mu)
		}
		if mus, ok := ms["mutations"]; ok && mus != nil {
			var mm []map[string]*skipJSONUnmarshal
			if err := json.Unmarshal(mus.bs, &mm); err != nil {
				jsonErr := convertJSONError(string(mus.bs), err)
				x.SetStatus(w, x.ErrorInvalidRequest, jsonErr.Error())
				return
			}

			for _, m := range mm {
				if mu, err := extractMutation(m); err != nil {
					x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
					return
				} else if mu != nil {
					req.Mutations = append(req.Mutations, mu)
				}
			}
		}

	case "application/rdf":
		// Parse N-Quads.
		req, err = dql.ParseMutation(string(body))
		if err != nil {
			x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
			return
		}

	default:
		x.SetStatus(w, x.ErrorInvalidRequest, "Unsupported Content-Type. "+
			"Supported content types are application/json, application/rdf")
		return
	}

	// end of query parsing
	parseEnd := time.Now()

	req.StartTs = startTs
	req.Hash = hash
	req.CommitNow = commitNow

	ctx := x.AttachAccessJwt(context.Background(), r)
	resp, err := (&edgraph.Server{}).QueryNoGrpc(ctx, req) // NOTE:核心操作，处理突变
	if err != nil {
		x.SetStatusWithData(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	// Add cost to the header.
	w.Header().Set(x.DgraphCostHeader, fmt.Sprint(resp.Metrics.NumUids["_total"]))

	resp.Latency.ParsingNs = uint64(parseEnd.Sub(parseStart).Nanoseconds())
	e := query.Extensions{
		Txn:     resp.Txn,
		Latency: resp.Latency,
	}
	sort.Strings(e.Txn.Keys)
	sort.Strings(e.Txn.Preds)

	// Don't send keys array which is part of txn context if its commit immediately.
	if req.CommitNow {
		e.Txn.Keys = e.Txn.Keys[:0]
	}

	response := map[string]interface{}{}
	response["extensions"] = e
	mp := map[string]interface{}{}
	mp["code"] = x.Success
	mp["message"] = "Done"
	mp["uids"] = resp.Uids
	mp["queries"] = json.RawMessage(resp.Json)
	response["data"] = mp

	js, err := json.Marshal(response)
	if err != nil {
		x.SetStatusWithData(w, x.Error, err.Error())
		return
	}

	_, _ = x.WriteResponse(w, r, js)
}

func commitHandler(w http.ResponseWriter, r *http.Request) {
	if commonHandler(w, r) {
		return
	}

	startTs, err := parseUint64(r, "startTs")
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	if startTs == 0 {
		x.SetStatus(w, x.ErrorInvalidRequest,
			"startTs parameter is mandatory while trying to commit")
		return
	}

	hash := r.URL.Query().Get("hash")
	abort, err := parseBool(r, "abort")
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}

	ctx := x.AttachAccessJwt(context.Background(), r)
	var response map[string]interface{}
	if abort {
		response, err = handleAbort(ctx, startTs, hash)
	} else {
		// Keys are sent as an array in the body.
		reqText := readRequest(w, r)
		if reqText == nil {
			return
		}

		response, err = handleCommit(ctx, startTs, hash, reqText)
	}
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}

	js, err := json.Marshal(response)
	if err != nil {
		x.SetStatusWithData(w, x.Error, err.Error())
		return
	}

	_, _ = x.WriteResponse(w, r, js)
}

func handleAbort(ctx context.Context, startTs uint64, hash string) (map[string]interface{}, error) {
	tc := &api.TxnContext{
		StartTs: startTs,
		Aborted: true,
		Hash:    hash,
	}

	tctx, err := (&edgraph.Server{}).CommitOrAbort(ctx, tc)
	switch {
	case tctx.Aborted:
		return map[string]interface{}{
			"code":    x.Success,
			"message": "Done",
		}, nil
	case err == nil:
		return nil, errors.Errorf("transaction could not be aborted")
	default:
		return nil, err
	}
}

func handleCommit(ctx context.Context,
	startTs uint64, hash string, reqText []byte) (map[string]interface{}, error) {
	tc := &api.TxnContext{
		StartTs: startTs,
		Hash:    hash,
	}

	var reqList []string
	useList := false
	if err := json.Unmarshal(reqText, &reqList); err == nil {
		useList = true
	}

	var reqMap map[string][]string
	if err := json.Unmarshal(reqText, &reqMap); err != nil && !useList {
		return nil, err
	}

	if useList {
		tc.Keys = reqList
	} else {
		tc.Keys = reqMap["keys"]
		tc.Preds = reqMap["preds"]
	}

	tc, err := (&edgraph.Server{}).CommitOrAbort(ctx, tc)
	if err != nil {
		return nil, err
	}

	resp := &api.Response{}
	resp.Txn = tc
	e := query.Extensions{
		Txn: resp.Txn,
	}
	e.Txn.Keys = e.Txn.Keys[:0]
	response := map[string]interface{}{}
	response["extensions"] = e
	mp := map[string]interface{}{}
	mp["code"] = x.Success
	mp["message"] = "Done"
	response["data"] = mp

	return response, nil
}

func alterHandler(w http.ResponseWriter, r *http.Request) {
	if commonHandler(w, r) {
		return
	}

	b := readRequest(w, r)
	if b == nil {
		return
	}

	op := &api.Operation{}
	if err := jsonpb.Unmarshal(b, op); err != nil {
		op.Schema = string(b)
	}

	runInBackground, err := parseBool(r, "runInBackground")
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	op.RunInBackground = runInBackground

	glog.Infof("Got alter request via HTTP from %s\n", r.RemoteAddr)
	fwd := r.Header.Get("X-Forwarded-For")
	if len(fwd) > 0 {
		glog.Infof("The alter request is forwarded by %s\n", fwd)
	}

	// Pass in PoorMan's auth, ACL and IP information if present.
	ctx := x.AttachAuthToken(context.Background(), r)
	ctx = x.AttachAccessJwt(ctx, r)
	ctx = x.AttachRemoteIP(ctx, r)
	if _, err := (&edgraph.Server{}).Alter(ctx, op); err != nil {
		x.SetStatus(w, x.Error, err.Error())
		return
	}

	writeSuccessResponse(w, r)
}

func adminSchemaHandler(w http.ResponseWriter, r *http.Request) {
	if commonHandler(w, r) {
		return
	}

	b := readRequest(w, r)
	if b == nil {
		return
	}

	gqlReq := &schema.Request{
		Query: `
		mutation updateGqlSchema($sch: String!) {
			updateGQLSchema(input: {
				set: {
					schema: $sch
				}
			}) {
				gqlSchema {
					id
				}
			}
		}`,
		Variables: map[string]interface{}{"sch": string(b)},
	}

	response := resolveWithAdminServer(gqlReq, r, adminServer)
	if len(response.Errors) > 0 {
		x.SetStatus(w, x.Error, response.Errors.Error())
		return
	}

	writeSuccessResponse(w, r)
}

func graphqlProbeHandler(gqlHealthStore *admin.GraphQLHealthStore, globalEpoch map[uint64]*uint64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		x.AddCorsHeaders(w)
		w.Header().Set("Content-Type", "application/json")
		// lazy load the schema so that just by making a probe request,
		// one can boot up GraphQL for their namespace
		namespace := x.ExtractNamespaceHTTP(r)
		if err := admin.LazyLoadSchema(namespace); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			x.Check2(w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err))))
			return
		}

		healthStatus := gqlHealthStore.GetHealth()
		httpStatusCode := http.StatusOK
		if !healthStatus.Healthy {
			httpStatusCode = http.StatusServiceUnavailable
		}
		w.WriteHeader(httpStatusCode)
		e := globalEpoch[namespace]
		var counter uint64
		if e != nil {
			counter = atomic.LoadUint64(e)
		}
		x.Check2(w.Write([]byte(fmt.Sprintf(`{"status":"%s","schemaUpdateCounter":%d}`,
			healthStatus.StatusMsg, counter))))
	})
}

func resolveWithAdminServer(gqlReq *schema.Request, r *http.Request,
	adminServer admin.IServeGraphQL) *schema.Response {
	md := metadata.New(nil)
	ctx := metadata.NewIncomingContext(context.Background(), md)
	ctx = x.AttachAccessJwt(ctx, r)
	ctx = x.AttachRemoteIP(ctx, r)
	ctx = x.AttachAuthToken(ctx, r)
	ctx = x.AttachJWTNamespace(ctx)

	return adminServer.ResolveWithNs(ctx, x.GalaxyNamespace, gqlReq)
}

func writeSuccessResponse(w http.ResponseWriter, r *http.Request) {
	res := map[string]interface{}{}
	data := map[string]interface{}{}
	data["code"] = x.Success
	data["message"] = "Done"
	res["data"] = data

	js, err := json.Marshal(res)
	if err != nil {
		x.SetStatus(w, x.Error, err.Error())
		return
	}

	_, _ = x.WriteResponse(w, r, js)
}

// skipJSONUnmarshal stores the raw bytes as is while JSON unmarshaling.
type skipJSONUnmarshal struct {
	bs []byte
}

func (sju *skipJSONUnmarshal) UnmarshalJSON(bs []byte) error {
	sju.bs = bs
	return nil
}

// convertJSONError adds line and character information to the JSON error.
// Idea taken from: https://bit.ly/2moFIVS
func convertJSONError(input string, err error) error {
	if err == nil {
		return nil
	}

	if jsonError, ok := err.(*json.SyntaxError); ok {
		line, character, lcErr := jsonLineAndChar(input, int(jsonError.Offset))
		if lcErr != nil {
			return err
		}
		return errors.Errorf("Error parsing JSON at line %d, character %d: %v\n", line, character,
			jsonError.Error())
	}

	if jsonError, ok := err.(*json.UnmarshalTypeError); ok {
		line, character, lcErr := jsonLineAndChar(input, int(jsonError.Offset))
		if lcErr != nil {
			return err
		}
		return errors.Errorf("Error parsing JSON at line %d, character %d: %v\n", line, character,
			jsonError.Error())
	}

	return err
}

func jsonLineAndChar(input string, offset int) (line int, character int, err error) {
	lf := rune(0x0A)

	if offset > len(input) || offset < 0 {
		return 0, 0, errors.Errorf("Couldn't find offset %d within the input.", offset)
	}

	line = 1
	for i, b := range input {
		if b == lf {
			line++
			character = 0
		}
		character++
		if i == offset {
			break
		}
	}

	return line, character, nil
}
