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

package edgraph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	ostats "go.opencensus.io/stats"
	"go.opencensus.io/tag"
	otrace "go.opencensus.io/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/dgraph-io/dgo/v240"
	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/hypermodeinc/dgraph/v24/chunker"
	"github.com/hypermodeinc/dgraph/v24/conn"
	"github.com/hypermodeinc/dgraph/v24/dql"
	gqlSchema "github.com/hypermodeinc/dgraph/v24/graphql/schema"
	"github.com/hypermodeinc/dgraph/v24/posting"
	"github.com/hypermodeinc/dgraph/v24/protos/pb"
	"github.com/hypermodeinc/dgraph/v24/query"
	"github.com/hypermodeinc/dgraph/v24/schema"
	"github.com/hypermodeinc/dgraph/v24/telemetry"
	"github.com/hypermodeinc/dgraph/v24/tok"
	"github.com/hypermodeinc/dgraph/v24/types"
	"github.com/hypermodeinc/dgraph/v24/types/facets"
	"github.com/hypermodeinc/dgraph/v24/worker"
	"github.com/hypermodeinc/dgraph/v24/x"
)

const (
	methodMutate = "Server.Mutate"
	methodQuery  = "Server.Query"
)

type GraphqlContextKey int

// uniquePredMeta stores the query variable name from 'uniqueQuery'
type uniquePredMeta struct {
	queryVar string
	valVar   string
}

const (
	// IsGraphql is used to validate requests which are allowed to mutate GraphQL reserved
	// predicates, like dgraph.graphql.schema and dgraph.graphql.xid.
	IsGraphql GraphqlContextKey = iota
	// Authorize is used to set if the request requires validation.
	Authorize
)

type AuthMode int

const (
	// NeedAuthorize is used to indicate that the request needs to be authorized.
	NeedAuthorize AuthMode = iota
	// NoAuthorize is used to indicate that authorization needs to be skipped.
	// Used when ACL needs to query information for performing the authorization check.
	NoAuthorize
)

// 下面是两个全局变量，用于统计请求的数量
var (
	numDQL     uint64
	numGraphQL uint64
)

var (
	errIndexingInProgress = errors.New("errIndexingInProgress. Please retry")
)

// Server implements protos.DgraphServer
type Server struct {
	// embedding the api.UnimplementedZeroServer struct to ensure forward compatibility of the server.
	api.UnimplementedDgraphServer
}

// graphQLSchemaNode represents the node which contains GraphQL schema
type graphQLSchemaNode struct {
	Uid    string `json:"uid"`
	UidInt uint64
	Schema string `json:"dgraph.graphql.schema"`
}

type existingGQLSchemaQryResp struct {
	ExistingGQLSchema []graphQLSchemaNode `json:"ExistingGQLSchema"`
}

// PeriodicallyPostTelemetry periodically reports telemetry data for alpha.
func PeriodicallyPostTelemetry() {
	glog.V(2).Infof("Starting telemetry data collection for alpha...")

	start := time.Now()
	ticker := time.NewTicker(time.Minute * 10)
	defer ticker.Stop()

	var lastPostedAt time.Time
	for range ticker.C {
		if time.Since(lastPostedAt) < time.Hour {
			continue
		}
		ms := worker.GetMembershipState()
		t := telemetry.NewAlpha(ms)
		t.NumDQL = atomic.SwapUint64(&numDQL, 0)
		t.NumGraphQL = atomic.SwapUint64(&numGraphQL, 0)
		t.SinceHours = int(time.Since(start).Hours())
		glog.V(2).Infof("Posting Telemetry data: %+v", t)

		err := t.Post()
		if err == nil {
			lastPostedAt = time.Now()
		} else {
			atomic.AddUint64(&numDQL, t.NumDQL)
			atomic.AddUint64(&numGraphQL, t.NumGraphQL)
			glog.V(2).Infof("Telemetry couldn't be posted. Error: %v", err)
		}
	}
}

// GetGQLSchema queries for the GraphQL schema node, and returns the uid and the GraphQL schema.
// If multiple schema nodes were found, it returns an error.
func GetGQLSchema(namespace uint64) (uid, graphQLSchema string, err error) {
	ctx := context.WithValue(context.Background(), Authorize, false)
	ctx = x.AttachNamespace(ctx, namespace)
	resp, err := (&Server{}).QueryNoGrpc(ctx,
		&api.Request{
			Query: `
			query {
				ExistingGQLSchema(func: has(dgraph.graphql.schema)) {
					uid
					dgraph.graphql.schema
				  }
				}`})
	if err != nil {
		return "", "", err
	}

	var result existingGQLSchemaQryResp
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return "", "", errors.Wrap(err, "Couldn't unmarshal response from Dgraph query")
	}
	res := result.ExistingGQLSchema
	if len(res) == 0 {
		// no schema has been stored yet in Dgraph
		return "", "", nil
	} else if len(res) == 1 {
		// we found an existing GraphQL schema
		gqlSchemaNode := res[0]
		return gqlSchemaNode.Uid, gqlSchemaNode.Schema, nil
	}

	// found multiple GraphQL schema nodes, this should never happen
	// returning the schema node which is added last
	for i := range res {
		iUid, err := dql.ParseUid(res[i].Uid)
		if err != nil {
			return "", "", err
		}
		res[i].UidInt = iUid
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].UidInt < res[j].UidInt
	})
	glog.Errorf("namespace: %d. Multiple schema nodes found, using the last one", namespace)
	resLast := res[len(res)-1]
	return resLast.Uid, resLast.Schema, nil
}

// UpdateGQLSchema updates the GraphQL and Dgraph schemas using the given inputs.
// It first validates and parses the dgraphSchema given in input. If that fails,
// it returns an error. All this is done on the alpha on which the update request is received.
// Then it sends an update request to the worker, which is executed only on Group-1 leader.
func UpdateGQLSchema(ctx context.Context, gqlSchema,
	dgraphSchema string) (*pb.UpdateGraphQLSchemaResponse, error) {
	var err error
	parsedDgraphSchema := &schema.ParsedSchema{}

	if !x.WorkerConfig.AclEnabled {
		ctx = x.AttachNamespace(ctx, x.GalaxyNamespace)
	}
	// The schema could be empty if it only has custom types/queries/mutations.
	if dgraphSchema != "" {
		op := &api.Operation{Schema: dgraphSchema}
		if err = validateAlterOperation(ctx, op); err != nil {
			return nil, err
		}
		if parsedDgraphSchema, err = parseSchemaFromAlterOperation(ctx, op); err != nil {
			return nil, err
		}
	}

	return worker.UpdateGQLSchemaOverNetwork(ctx, &pb.UpdateGraphQLSchemaRequest{
		StartTs:       worker.State.GetTimestamp(false),
		GraphqlSchema: gqlSchema,
		DgraphPreds:   parsedDgraphSchema.Preds,
		DgraphTypes:   parsedDgraphSchema.Types,
	})
}

// validateAlterOperation validates the given operation for alter.
func validateAlterOperation(ctx context.Context, op *api.Operation) error {
	// The following code block checks if the operation should run or not.
	if op.Schema == "" && op.DropAttr == "" && !op.DropAll && op.DropOp == api.Operation_NONE {
		// Must have at least one field set. This helps users if they attempt
		// to set a field but use the wrong name (could be decoded from JSON).
		return errors.Errorf("Operation must have at least one field set")
	}
	if err := x.HealthCheck(); err != nil {
		return err
	}

	if isDropAll(op) && op.DropOp == api.Operation_DATA {
		return errors.Errorf("Only one of DropAll and DropData can be true")
	}

	if !isMutationAllowed(ctx) {
		return errors.Errorf("No mutations allowed by server.")
	}

	if _, err := hasAdminAuth(ctx, "Alter"); err != nil {
		glog.Warningf("Alter denied with error: %v\n", err)
		return err
	}

	if err := authorizeAlter(ctx, op); err != nil {
		glog.Warningf("Alter denied with error: %v\n", err)
		return err
	}

	return nil
}

// parseSchemaFromAlterOperation parses the string schema given in input operation to a Go
// struct, and performs some checks to make sure that the schema is valid.
func parseSchemaFromAlterOperation(ctx context.Context, op *api.Operation) (
	*schema.ParsedSchema, error) {

	// If a background task is already running, we should reject all the new alter requests.
	if schema.State().IndexingInProgress() {
		return nil, errIndexingInProgress
	}

	namespace, err := x.ExtractNamespace(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "While parsing schema")
	}

	if x.IsGalaxyOperation(ctx) {
		// Only the guardian of the galaxy can do a galaxy wide query/mutation. This operation is
		// needed by live loader.
		if err := AuthGuardianOfTheGalaxy(ctx); err != nil {
			s := status.Convert(err)
			return nil, status.Error(s.Code(),
				"Non guardian of galaxy user cannot bypass namespaces. "+s.Message())
		}
		var err error
		namespace, err = strconv.ParseUint(x.GetForceNamespace(ctx), 0, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "Valid force namespace not found in metadata")
		}
	}

	result, err := schema.ParseWithNamespace(op.Schema, namespace)
	if err != nil {
		return nil, err
	}

	preds := make(map[string]struct{})
	for _, update := range result.Preds {
		if _, ok := preds[update.Predicate]; ok {
			return nil, errors.Errorf("predicate %s defined multiple times", x.ParseAttr(update.Predicate))
		}
		preds[update.Predicate] = struct{}{}

		// Pre-defined predicates cannot be altered but let the update go through
		// if the update is equal to the existing one.
		if schema.CheckAndModifyPreDefPredicate(update) {
			return nil, errors.Errorf("predicate %s is pre-defined and is not allowed to be"+
				" modified", x.ParseAttr(update.Predicate))
		}

		if err := validatePredName(update.Predicate); err != nil {
			return nil, err
		}
		// Users are not allowed to create a predicate under the reserved `dgraph.` namespace. But,
		// there are pre-defined predicates (subset of reserved predicates), and for them we allow
		// the schema update to go through if the update is equal to the existing one.
		// So, here we check if the predicate is reserved but not pre-defined to block users from
		// creating predicates in reserved namespace.
		if x.IsReservedPredicate(update.Predicate) && !x.IsPreDefinedPredicate(update.Predicate) {
			return nil, errors.Errorf("Can't alter predicate `%s` as it is prefixed with `dgraph.`"+
				" which is reserved as the namespace for dgraph's internal types/predicates.",
				x.ParseAttr(update.Predicate))
		}
	}

	types := make(map[string]struct{})
	for _, typ := range result.Types {
		if _, ok := types[typ.TypeName]; ok {
			return nil, errors.Errorf("type %s defined multiple times", x.ParseAttr(typ.TypeName))
		}
		types[typ.TypeName] = struct{}{}

		// Pre-defined types cannot be altered but let the update go through
		// if the update is equal to the existing one.
		if schema.IsPreDefTypeChanged(typ) {
			return nil, errors.Errorf("type %s is pre-defined and is not allowed to be modified",
				x.ParseAttr(typ.TypeName))
		}

		// Users are not allowed to create types in reserved namespace. But, there are pre-defined
		// types for which the update should go through if the update is equal to the existing one.
		if x.IsReservedType(typ.TypeName) && !x.IsPreDefinedType(typ.TypeName) {
			return nil, errors.Errorf("Can't alter type `%s` as it is prefixed with `dgraph.` "+
				"which is reserved as the namespace for dgraph's internal types/predicates.",
				x.ParseAttr(typ.TypeName))
		}
	}

	return result, nil
}

// InsertDropRecord is used to insert a helper record when a DROP operation is performed.
// This helper record lets us know during backup that a DROP operation was performed and that we
// need to write this information in backup manifest. So that while restoring from a backup series,
// we can create an exact replica of the system which existed at the time the last backup was taken.
// Note that if the server crashes after the DROP operation & before this helper record is inserted,
// then restoring from the incremental backup of such a DB would restore even the dropped
// data back. This is also used to capture the delete namespace operation during backup.
func InsertDropRecord(ctx context.Context, dropOp string) error {
	_, err := (&Server{}).doQuery(context.WithValue(ctx, IsGraphql, true), &Request{
		req: &api.Request{
			Mutations: []*api.Mutation{{
				Set: []*api.NQuad{{
					Subject:     "_:r",
					Predicate:   "dgraph.drop.op",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: dropOp}},
				}},
			}},
			CommitNow: true,
		}, doAuth: NoAuthorize})
	return err
}

// Alter handles requests to change the schema or remove parts or all of the data.
func (s *Server) Alter(ctx context.Context, op *api.Operation) (*api.Payload, error) {
	ctx, span := otrace.StartSpan(ctx, "Server.Alter")
	defer span.End()

	ctx = x.AttachJWTNamespace(ctx)
	span.Annotatef(nil, "Alter operation: %+v", op)

	// Always print out Alter operations because they are important and rare.
	glog.Infof("Received ALTER op: %+v", op)

	// check if the operation is valid
	if err := validateAlterOperation(ctx, op); err != nil {
		return nil, err
	}

	defer glog.Infof("ALTER op: %+v done", op)

	empty := &api.Payload{}
	namespace, err := x.ExtractNamespace(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "While altering")
	}

	// StartTs is not needed if the predicate to be dropped lies on this server but is required
	// if it lies on some other machine. Let's get it for safety.
	m := &pb.Mutations{StartTs: worker.State.GetTimestamp(false)}
	if isDropAll(op) {
		if x.Config.BlockClusterWideDrop {
			glog.V(2).Info("Blocked drop-all because it is not permitted.")
			return empty, errors.New("Drop all operation is not permitted.")
		}
		if err := AuthGuardianOfTheGalaxy(ctx); err != nil {
			s := status.Convert(err)
			return empty, status.Error(s.Code(),
				"Drop all can only be called by the guardian of the galaxy. "+s.Message())
		}
		if len(op.DropValue) > 0 {
			return empty, errors.Errorf("If DropOp is set to ALL, DropValue must be empty")
		}

		m.DropOp = pb.Mutations_ALL
		_, err := query.ApplyMutations(ctx, m)
		if err != nil {
			return empty, err
		}

		// insert a helper record for backup & restore, indicating that drop_all was done
		err = InsertDropRecord(ctx, "DROP_ALL;")
		if err != nil {
			return empty, err
		}

		// insert empty GraphQL schema, so all alphas get notified to
		// reset their in-memory GraphQL schema
		_, err = UpdateGQLSchema(ctx, "", "")
		// recreate the admin account after a drop all operation
		InitializeAcl(nil)
		return empty, err
	}

	if op.DropOp == api.Operation_DATA {
		if len(op.DropValue) > 0 {
			return empty, errors.Errorf("If DropOp is set to DATA, DropValue must be empty")
		}

		// query the GraphQL schema and keep it in memory, so it can be inserted again
		_, graphQLSchema, err := GetGQLSchema(namespace)
		if err != nil {
			return empty, err
		}

		m.DropOp = pb.Mutations_DATA
		m.DropValue = fmt.Sprintf("%#x", namespace)
		_, err = query.ApplyMutations(ctx, m)
		if err != nil {
			return empty, err
		}

		// insert a helper record for backup & restore, indicating that drop_data was done
		err = InsertDropRecord(ctx, fmt.Sprintf("DROP_DATA;%#x", namespace))
		if err != nil {
			return empty, err
		}

		// just reinsert the GraphQL schema, no need to alter dgraph schema as this was drop_data
		_, err = UpdateGQLSchema(ctx, graphQLSchema, "")

		// Since all data has been dropped, we need to recreate the admin account in the respective namespace.
		upsertGuardianAndGroot(nil, namespace)
		return empty, err
	}

	if len(op.DropAttr) > 0 || op.DropOp == api.Operation_ATTR {
		if op.DropOp == api.Operation_ATTR && op.DropValue == "" {
			return empty, errors.Errorf("If DropOp is set to ATTR, DropValue must not be empty")
		}

		var attr string
		if len(op.DropAttr) > 0 {
			attr = op.DropAttr
		} else {
			attr = op.DropValue
		}
		attr = x.NamespaceAttr(namespace, attr)
		// Pre-defined predicates cannot be dropped.
		if x.IsPreDefinedPredicate(attr) {
			return empty, errors.Errorf("predicate %s is pre-defined and is not allowed to be"+
				" dropped", x.ParseAttr(attr))
		}

		nq := &api.NQuad{
			Subject:     x.Star,
			Predicate:   x.ParseAttr(attr),
			ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: x.Star}},
		}
		wnq := &dql.NQuad{NQuad: nq}
		edge, err := wnq.ToDeletePredEdge()
		if err != nil {
			return empty, err
		}
		edges := []*pb.DirectedEdge{edge}
		m.Edges = edges
		_, err = query.ApplyMutations(ctx, m)
		if err != nil {
			return empty, err
		}

		// insert a helper record for backup & restore, indicating that drop_attr was done
		err = InsertDropRecord(ctx, "DROP_ATTR;"+attr)
		return empty, err
	}

	if op.DropOp == api.Operation_TYPE {
		if op.DropValue == "" {
			return empty, errors.Errorf("If DropOp is set to TYPE, DropValue must not be empty")
		}

		// Pre-defined types cannot be dropped.
		dropPred := x.NamespaceAttr(namespace, op.DropValue)
		if x.IsPreDefinedType(dropPred) {
			return empty, errors.Errorf("type %s is pre-defined and is not allowed to be dropped",
				op.DropValue)
		}

		m.DropOp = pb.Mutations_TYPE
		m.DropValue = dropPred
		_, err := query.ApplyMutations(ctx, m)
		return empty, err
	}
	result, err := parseSchemaFromAlterOperation(ctx, op)
	if err == errIndexingInProgress {
		// Make the client wait a bit.
		time.Sleep(time.Second)
		return nil, err
	} else if err != nil {
		return nil, err
	}

	glog.Infof("Got schema: %+v\n", result)
	// TODO: Maybe add some checks about the schema.
	m.Schema = result.Preds
	m.Types = result.Types
	_, err = query.ApplyMutations(ctx, m)
	if err != nil {
		return empty, err
	}

	// wait for indexing to complete or context to be canceled.
	if err = worker.WaitForIndexing(ctx, !op.RunInBackground); err != nil {
		return empty, err
	}

	return empty, nil
}

func annotateNamespace(span *otrace.Span, ns uint64) {
	span.AddAttributes(otrace.Int64Attribute("ns", int64(ns)))
}

func annotateStartTs(span *otrace.Span, ts uint64) {
	span.AddAttributes(otrace.Int64Attribute("startTs", int64(ts)))
}

func (s *Server) doMutate(ctx context.Context, qc *queryContext, resp *api.Response) error {

	if len(qc.gmuList) == 0 {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	start := time.Now()
	defer func() {
		qc.latency.Processing += time.Since(start)
	}()

	if !isMutationAllowed(ctx) {
		return errors.Errorf("no mutations allowed")
	}

	// update mutations from the query results before assigning UIDs
	if err := updateMutations(qc); err != nil {
		return err
	}

	if err := verifyUniqueWithinMutation(qc); err != nil {
		return err
	}

	newUids, err := query.AssignUids(ctx, qc.gmuList)
	if err != nil {
		return err
	}

	// resp.Uids contains a map of the node name to the uid.
	// 1. For a blank node, like _:foo, the key would be foo.
	// 2. For a uid variable that is part of an upsert query,
	//    like uid(foo), the key would be uid(foo).
	resp.Uids = query.UidsToHex(query.StripBlankNode(newUids))
	edges, err := query.ToDirectedEdges(qc.gmuList, newUids)
	if err != nil {
		return err
	}

	if len(edges) > x.Config.LimitMutationsNquad {
		return errors.Errorf("NQuad count in the request: %d, is more that threshold: %d",
			len(edges), x.Config.LimitMutationsNquad)
	}

	ns, err := x.ExtractNamespace(ctx)
	if err != nil {
		return errors.Wrapf(err, "While doing mutations:")
	}
	predHints := make(map[string]pb.Metadata_HintType)
	for _, gmu := range qc.gmuList {
		for pred, hint := range gmu.Metadata.GetPredHints() {
			pred = x.NamespaceAttr(ns, pred)
			if oldHint := predHints[pred]; oldHint == pb.Metadata_LIST {
				continue
			}
			predHints[pred] = hint
		}
	}
	m := &pb.Mutations{
		Edges:   edges,
		StartTs: qc.req.StartTs,
		Metadata: &pb.Metadata{
			PredHints: predHints,
		},
	}

	// ensure that we do not insert very large (> 64 KB) value
	if err := validateMutation(ctx, edges); err != nil {
		return err
	}

	qc.span.Annotatef(nil, "Applying mutations: %+v", m)
	resp.Txn, err = query.ApplyMutations(ctx, m)
	qc.span.Annotatef(nil, "Txn Context: %+v. Err=%v", resp.Txn, err)

	// calculateMutationMetrics calculate cost for the mutation.
	calculateMutationMetrics := func() {
		cost := uint64(len(newUids) + len(edges))
		resp.Metrics.NumUids["mutation_cost"] = cost
		resp.Metrics.NumUids["_total"] = resp.Metrics.NumUids["_total"] + cost
	}
	if !qc.req.CommitNow {
		calculateMutationMetrics()
		if err == x.ErrConflict {
			err = status.Error(codes.FailedPrecondition, err.Error())
		}

		return err
	}

	// The following logic is for committing immediately.
	if err != nil {
		// ApplyMutations failed. We now want to abort the transaction,
		// ignoring any error that might occur during the abort (the user would
		// care more about the previous error).
		if resp.Txn == nil {
			resp.Txn = &api.TxnContext{StartTs: qc.req.StartTs}
		}

		resp.Txn.Aborted = true
		_, _ = worker.CommitOverNetwork(ctx, resp.Txn)

		if err == x.ErrConflict {
			// We have already aborted the transaction, so the error message should reflect that.
			return dgo.ErrAborted
		}

		return err
	}

	qc.span.Annotatef(nil, "Prewrites err: %v. Attempting to commit/abort immediately.", err)
	ctxn := resp.Txn
	// zero would assign the CommitTs
	cts, err := worker.CommitOverNetwork(ctx, ctxn)
	qc.span.Annotatef(nil, "Status of commit at ts: %d: %v", ctxn.StartTs, err)
	if err != nil {
		if err == dgo.ErrAborted {
			err = status.Errorf(codes.Aborted, err.Error())
			resp.Txn.Aborted = true
		}

		return err
	}

	// CommitNow was true, no need to send keys.
	resp.Txn.Keys = resp.Txn.Keys[:0]
	resp.Txn.CommitTs = cts
	calculateMutationMetrics()
	return nil
}

// validateMutation ensures that the value in the edge is not too big.
// The challange here is that the keys in badger have a limitation on their size (< 2<<16).
// We need to ensure that no key, either primary or secondary index key is bigger than that.
// See here for more details: https://github.com/dgraph-io/projects/issues/73
func validateMutation(ctx context.Context, edges []*pb.DirectedEdge) error {
	errValueTooBigForIndex := errors.New("value in the mutation is too large for the index")

	// key = meta data + predicate + actual key, this all needs to fit into 64 KB
	// we are keeping 536 bytes aside for meta information we put into the key and we
	// use 65000 bytes for the rest, that is predicate and the actual key.
	const maxKeySize = 65000

	for _, e := range edges {
		maxSizeForDataKey := maxKeySize - len(e.Attr)

		// seems reasonable to assume, the tokens for indexes won't be bigger than the value itself
		if len(e.Value) <= maxSizeForDataKey {
			continue
		}
		pred := x.NamespaceAttr(e.Namespace, e.Attr)
		update, ok := schema.State().Get(ctx, pred)
		if !ok {
			continue
		}
		// only string type can have large values that could cause us issues later
		if update.GetValueType() != pb.Posting_STRING {
			continue
		}

		storageVal := types.Val{Tid: types.TypeID(e.GetValueType()), Value: e.GetValue()}
		schemaVal, err := types.Convert(storageVal, types.TypeID(update.GetValueType()))
		if err != nil {
			return err
		}

		for _, tokenizer := range schema.State().Tokenizer(ctx, pred) {
			toks, err := tok.BuildTokens(schemaVal.Value, tok.GetTokenizerForLang(tokenizer, e.Lang))
			if err != nil {
				return fmt.Errorf("error while building index tokens: %w", err)
			}

			for _, tok := range toks {
				if len(tok) > maxSizeForDataKey {
					return errValueTooBigForIndex
				}
			}
		}
	}

	return nil
}

// buildUpsertQuery modifies the query to evaluate the
// @if condition defined in Conditional Upsert.
func buildUpsertQuery(qc *queryContext) string {
	if qc.req.Query == "" || len(qc.gmuList) == 0 {
		return qc.req.Query
	}

	qc.condVars = make([]string, len(qc.req.Mutations))

	var upsertQB strings.Builder
	x.Check2(upsertQB.WriteString(strings.TrimSuffix(qc.req.Query, "}")))

	for i, gmu := range qc.gmuList {
		isCondUpsert := strings.TrimSpace(gmu.Cond) != ""
		if isCondUpsert {
			qc.condVars[i] = fmt.Sprintf("__dgraph_upsertcheck_%v__", strconv.Itoa(i))
			qc.uidRes[qc.condVars[i]] = nil
			// @if in upsert is same as @filter in the query
			cond := strings.Replace(gmu.Cond, "@if", "@filter", 1)

			// Add dummy query to evaluate the @if directive, ok to use uid(0) because
			// dgraph doesn't check for existence of UIDs until we query for other predicates.
			// Here, we are only querying for uid predicate in the dummy query.
			//
			// For example if - mu.Query = {
			//      me(...) {...}
			//   }
			//
			// Then, upsertQuery = {
			//      me(...) {...}
			//      __dgraph_0__ as var(func: uid(0)) @filter(...)
			//   }
			//
			// The variable __dgraph_0__ will -
			//      * be empty if the condition is true
			//      * have 1 UID (the 0 UID) if the condition is false
			x.Check2(upsertQB.WriteString(qc.condVars[i]))
			x.Check2(upsertQB.WriteString(` as var(func: uid(0)) `))
			x.Check2(upsertQB.WriteString(cond))
			x.Check2(upsertQB.WriteString("\n"))
		}
	}

	x.Check2(upsertQB.WriteString(`}`))
	return upsertQB.String()
}

// updateMutations updates the mutation and replaces uid(var) and val(var) with
// their values or a blank node, in case of an upsert.
// We use the values stored in qc.uidRes and qc.valRes to update the mutation.
func updateMutations(qc *queryContext) error {
	for i, condVar := range qc.condVars {
		gmu := qc.gmuList[i]
		if condVar != "" {
			uids, ok := qc.uidRes[condVar]
			if !(ok && len(uids) == 1) {
				gmu.Set = nil
				gmu.Del = nil
				continue
			}
		}

		if err := updateUIDInMutations(gmu, qc); err != nil {
			return err
		}
		if err := updateValInMutations(gmu, qc); err != nil {
			return err
		}
	}

	return nil
}

// findMutationVars finds all the variables used in mutation block and stores them
// qc.uidRes and qc.valRes so that we only look for these variables in query results.
func findMutationVars(qc *queryContext) []string {
	updateVars := func(s string) {
		if strings.HasPrefix(s, "uid(") {
			varName := s[4 : len(s)-1]
			qc.uidRes[varName] = nil
		} else if strings.HasPrefix(s, "val(") {
			varName := s[4 : len(s)-1]
			qc.valRes[varName] = nil
		}
	}

	for _, gmu := range qc.gmuList {
		for _, nq := range gmu.Set {
			updateVars(nq.Subject)
			updateVars(nq.ObjectId)
		}
		for _, nq := range gmu.Del {
			updateVars(nq.Subject)
			updateVars(nq.ObjectId)
		}
	}

	varsList := make([]string, 0, len(qc.uidRes)+len(qc.valRes))
	for v := range qc.uidRes {
		varsList = append(varsList, v)
	}
	for v := range qc.valRes {
		varsList = append(varsList, v)
	}

	// We use certain variables to check uniqueness. To prevent errors related to unused
	// variables, we explicitly add them to the 'varsList' since they are not used in the mutation.
	for _, uniqueQueryVar := range qc.uniqueVars {
		varsList = append(varsList, uniqueQueryVar.queryVar)
		// If the triple contains a "val()" in objectId, we need to add
		// one more variable to the unique query. So, we explicitly add it to the varList.
		if uniqueQueryVar.valVar != "" {
			varsList = append(varsList, uniqueQueryVar.valVar)
		}
	}

	return varsList
}

// updateValInNQuads picks the val() from object and replaces it with its value
// Assumption is that Subject can contain UID, whereas Object can contain Val
// If val(variable) exists in a query, but the values are not there for the variable,
// it will ignore the mutation silently.
func updateValInNQuads(nquads []*api.NQuad, qc *queryContext, isSet bool) []*api.NQuad {
	getNewVals := func(s string) (map[uint64]types.Val, bool) {
		if strings.HasPrefix(s, "val(") {
			varName := s[4 : len(s)-1]
			if v, ok := qc.valRes[varName]; ok && v != nil {
				return v, true
			}
			return nil, true
		}
		return nil, false
	}

	getValue := func(key uint64, uidToVal map[uint64]types.Val) (types.Val, bool) {
		val, ok := uidToVal[key]
		if ok {
			return val, true
		}

		// Check if the variable is aggregate variable
		// Only 0 key would exist for aggregate variable
		val, ok = uidToVal[0]
		return val, ok
	}

	newNQuads := nquads[:0]
	for _, nq := range nquads {
		// Check if the nquad contains a val() in Object or not.
		// If not then, keep the mutation and continue
		uidToVal, found := getNewVals(nq.ObjectId)
		if !found {
			newNQuads = append(newNQuads, nq)
			continue
		}

		// uid(u) <amount> val(amt)
		// For each NQuad, we need to convert the val(variable_name)
		// to *api.Value before applying the mutation. For that, first
		// we convert key to uint64 and get the UID to Value map from
		// the result of the query.
		var key uint64
		var err error
		switch {
		case nq.Subject[0] == '_' && isSet:
			// in case aggregate val(var) is there, that should work with blank node.
			key = 0
		case nq.Subject[0] == '_' && !isSet:
			// UID is of format "_:uid(u)". Ignore the delete silently
			continue
		default:
			key, err = strconv.ParseUint(nq.Subject, 0, 64)
			if err != nil {
				// Key conversion failed, ignoring the nquad. Ideally,
				// it shouldn't happen as this is the result of a query.
				glog.Errorf("Conversion of subject %s failed. Error: %s",
					nq.Subject, err.Error())
				continue
			}
		}

		// Get the value to the corresponding UID(key) from the query result
		nq.ObjectId = ""
		val, ok := getValue(key, uidToVal)
		if !ok {
			continue
		}

		// Convert the value from types.Val to *api.Value
		nq.ObjectValue, err = types.ObjectValue(val.Tid, val.Value)
		if err != nil {
			// Value conversion failed, ignoring the nquad. Ideally,
			// it shouldn't happen as this is the result of a query.
			glog.Errorf("Conversion of %s failed for %d subject. Error: %s",
				nq.ObjectId, key, err.Error())
			continue
		}

		newNQuads = append(newNQuads, nq)
	}
	qc.nquadsCount += len(newNQuads)
	return newNQuads
}

// updateValInMutations does following transformations:
// 0x123 <amount> val(v) -> 0x123 <amount> 13.0
func updateValInMutations(gmu *dql.Mutation, qc *queryContext) error {
	gmu.Del = updateValInNQuads(gmu.Del, qc, false)
	gmu.Set = updateValInNQuads(gmu.Set, qc, true)
	if qc.nquadsCount > x.Config.LimitMutationsNquad {
		return errors.Errorf("NQuad count in the request: %d, is more that threshold: %d",
			qc.nquadsCount, x.Config.LimitMutationsNquad)
	}
	return nil
}

// updateUIDInMutations does following transformations:
//   - uid(v) -> 0x123     -- If v is defined in query block
//   - uid(v) -> _:uid(v)  -- Otherwise
func updateUIDInMutations(gmu *dql.Mutation, qc *queryContext) error {
	// usedMutationVars keeps track of variables that are used in mutations.
	getNewVals := func(s string) []string {
		if strings.HasPrefix(s, "uid(") {
			varName := s[4 : len(s)-1]
			if uids, ok := qc.uidRes[varName]; ok && len(uids) != 0 {
				return uids
			}

			return []string{"_:" + s}
		}

		return []string{s}
	}

	getNewNQuad := func(nq *api.NQuad, s, o string) *api.NQuad {
		// The following copy is fine because we only modify Subject and ObjectId.
		// The pointer values are not modified across different copies of NQuad.
		n := *nq

		n.Subject = s
		n.ObjectId = o
		return &n
	}

	// Remove the mutations from gmu.Del when no UID was found.
	gmuDel := make([]*api.NQuad, 0, len(gmu.Del))
	for _, nq := range gmu.Del {
		// if Subject or/and Object are variables, each NQuad can result
		// in multiple NQuads if any variable stores more than one UIDs.
		newSubs := getNewVals(nq.Subject)
		newObs := getNewVals(nq.ObjectId)

		for _, s := range newSubs {
			for _, o := range newObs {
				// Blank node has no meaning in case of deletion.
				if strings.HasPrefix(s, "_:uid(") ||
					strings.HasPrefix(o, "_:uid(") {
					continue
				}

				gmuDel = append(gmuDel, getNewNQuad(nq, s, o))
				qc.nquadsCount++
			}
			if qc.nquadsCount > x.Config.LimitMutationsNquad {
				return errors.Errorf("NQuad count in the request: %d, is more that threshold: %d",
					qc.nquadsCount, x.Config.LimitMutationsNquad)
			}
		}
	}

	gmu.Del = gmuDel

	// Update the values in mutation block from the query block.
	gmuSet := make([]*api.NQuad, 0, len(gmu.Set))
	for _, nq := range gmu.Set {
		newSubs := getNewVals(nq.Subject)
		newObs := getNewVals(nq.ObjectId)

		qc.nquadsCount += len(newSubs) * len(newObs)
		if qc.nquadsCount > int(x.Config.LimitQueryEdge) {
			return errors.Errorf("NQuad count in the request: %d, is more that threshold: %d",
				qc.nquadsCount, int(x.Config.LimitQueryEdge))
		}

		for _, s := range newSubs {
			for _, o := range newObs {
				gmuSet = append(gmuSet, getNewNQuad(nq, s, o))
			}
		}
	}
	gmu.Set = gmuSet
	return nil
}

// queryContext is used to pass around all the variables needed
// to process a request for query, mutation or upsert.
type queryContext struct {
	// req is the incoming, not yet parsed request containing
	// a query or more than one mutations or both (in case of upsert)
	// 尚未解析的请求, 包含一个查询或多个突变或两者都有(在 upsert 的情况下)
	req *api.Request	

	// gmuList is the list of mutations after parsing req.Mutations
	// 解析 req.Mutations 后的突变列表
	gmuList []*dql.Mutation  

	// dqlRes contains result of parsing the req.Query
	// dqlRes保存 解析 req.Query 后的结果
	dqlRes dql.Result 

	// condVars are conditional variables used in the (modified) query to figure out
	// whether the condition in Conditional Upsert is true. The string would be empty
	// if the corresponding mutation is not a conditional upsert.
	// Note that, len(condVars) == len(gmuList).
	// 查询(修改后的)中使用的条件变量, 用于确定 Upsert 中的条件是否为真。如果相应的突变不是条件 upsert, 则该字符串将为空。
	//  请注意, len(condVars) == len(gmuList)
	condVars []string

	// uidRes stores mapping from variable names to UIDs for UID variables.
	// These variables are either dummy variables used for Conditional
	// Upsert or variables used in the mutation block in the incoming request.
	// 存储从变量名称到 UID 变量的 UID 的映射。 这些变量要么是用于 Upsert 条件的虚拟变量, 要么是用于传入请求的突变块中的变量
	uidRes map[string][]string

	// valRes stores mapping from variable names to values for value
	// variables used in the mutation block of incoming request.
	// 存储 mutation 请求块中使用的变量(从变量名到值)的映射
	valRes map[string]map[uint64]types.Val

	// l stores latency numbers
	// 存储各种操作所花费的时间
	latency *query.Latency

	// span stores a opencensus span used throughout the query processing
	// 存储在整个查询处理过程中使用的 opencensus span
	span *otrace.Span

	// graphql indicates whether the given request is from graphql admin or not.
	// 指示给定的请求是否来自 graphql 管理员。
	graphql bool

	// gqlField stores the GraphQL field for which the query is being processed.
	// This would be set only if the request is a query from GraphQL layer,
	// otherwise it would be nil. (Eg. nil cases: in case of a DQL query,
	// a mutation being executed from GraphQL layer).
	// 存储正在处理查询的 GraphQL 字段。仅当请求是来自 GraphQL 层的查询时才会设置, 否则将为 nil。例如 nil 案例: 在 DQL 查询的情况下, 从 GraphQL 层执行突变
	gqlField gqlSchema.Field

	// nquadsCount maintains numbers of nquads which would be inserted as part of this request.
	// In some cases(mostly upserts), numbers of nquads to be inserted can to huge(we have seen upto
	// 1B) and resulting in OOM. We are limiting number of nquads which can be inserted in
	// a single request.
	// 维护将作为此请求的一部分插入的 nquad 数量。在某些情况下(主要是 upserts), 要插入的 nquad 数量可能会很大(我们已经看到高达 1B)并导致 OOM。
	// 我们限制了可以在单个请求中插入的 nquad 数量。
	nquadsCount int

	// uniqueVar stores the mapping between the indexes of gmuList and gmu.Set,
	// along with their respective uniqueQueryVariables.
	// uniqueVar存储gmuList和gmu索引之间的映射。Set，以及它们各自的uniqueQueryVariables。
	uniqueVars map[uint64]uniquePredMeta
}

// Request represents a query request sent to the doQuery() method on the Server.
// It contains all the metadata required to execute a query.
type Request struct {
	// req is the incoming gRPC request
	req *api.Request
	// gqlField is the GraphQL field for which the request is being sent
	gqlField gqlSchema.Field
	// doAuth tells whether this request needs ACL authorization or not
	// doAuth告诉此请求是否需要ACL授权
	doAuth AuthMode
}

// Health handles /health and /health?all requests.
func (s *Server) Health(ctx context.Context, all bool) (*api.Response, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var healthAll []pb.HealthInfo
	if all {
		if err := AuthorizeGuardians(ctx); err != nil {
			return nil, err
		}
		pool := conn.GetPools().GetAll()
		for _, p := range pool {
			if p.Addr == x.WorkerConfig.MyAddr {
				continue
			}
			healthAll = append(healthAll, p.HealthInfo())
		}
	}

	// Append self.
	healthAll = append(healthAll, pb.HealthInfo{
		Instance:    "alpha",
		Address:     x.WorkerConfig.MyAddr,
		Status:      "healthy",
		Group:       strconv.Itoa(int(worker.GroupId())),
		Version:     x.Version(),
		Uptime:      int64(time.Since(x.WorkerConfig.StartTime) / time.Second),
		LastEcho:    time.Now().Unix(),
		Ongoing:     worker.GetOngoingTasks(),
		Indexing:    schema.GetIndexingPredicates(),
		EeFeatures:  worker.GetEEFeaturesList(),
		MaxAssigned: posting.Oracle().MaxAssigned(),
	})

	var err error
	var jsonOut []byte
	if jsonOut, err = json.Marshal(healthAll); err != nil {
		return nil, errors.Errorf("Unable to Marshal. Err %v", err)
	}
	return &api.Response{Json: jsonOut}, nil
}

// Filter out the tablets that do not belong to the requestor's namespace.
func filterTablets(ctx context.Context, ms *pb.MembershipState) error {
	if !x.WorkerConfig.AclEnabled {
		return nil
	}
	namespace, err := x.ExtractNamespaceFrom(ctx)
	if err != nil {
		return errors.Errorf("Namespace not found in JWT.")
	}
	if namespace == x.GalaxyNamespace {
		// For galaxy namespace, we don't want to filter out the predicates.
		return nil
	}
	for _, group := range ms.GetGroups() {
		tablets := make(map[string]*pb.Tablet)
		for pred, tablet := range group.GetTablets() {
			if ns, attr := x.ParseNamespaceAttr(pred); namespace == ns {
				tablets[attr] = tablet
				tablets[attr].Predicate = attr
			}
		}
		group.Tablets = tablets
	}
	return nil
}

// State handles state requests
func (s *Server) State(ctx context.Context) (*api.Response, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if err := AuthorizeGuardians(ctx); err != nil {
		return nil, err
	}

	ms := worker.GetMembershipState()
	if ms == nil {
		return nil, errors.Errorf("No membership state found")
	}

	if err := filterTablets(ctx, ms); err != nil {
		return nil, err
	}

	m := protojson.MarshalOptions{EmitUnpopulated: true}
	jsonState, err := m.Marshal(ms)
	if err != nil {
		return nil, errors.Errorf("Error marshalling state information to JSON")
	}

	return &api.Response{Json: jsonState}, nil
}

func getAuthMode(ctx context.Context) AuthMode {
	if auth := ctx.Value(Authorize); auth == nil || auth.(bool) {
		return NeedAuthorize
	}
	return NoAuthorize
}

// QueryGraphQL handles only GraphQL queries, neither mutations nor DQL.
// QueryGraphQL只处理GraphQL查询，既不处理突变也不处理DQL。
func (s *Server) QueryGraphQL(ctx context.Context, req *api.Request,
	field gqlSchema.Field) (*api.Response, error) {
	// Add a timeout for queries which don't have a deadline set. We don't want to
	// apply a timeout if it's a mutation, that's currently handled by flag
	// "txn-abort-after".
	if req.GetMutations() == nil && x.Config.QueryTimeout != 0 {
		if d, _ := ctx.Deadline(); d.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, x.Config.QueryTimeout)
			defer cancel()
		}
	}
	// no need to attach namespace here, it is already done by GraphQL layer
	return s.doQuery(ctx, &Request{req: req, gqlField: field, doAuth: getAuthMode(ctx)})
}

func (s *Server) Query(ctx context.Context, req *api.Request) (*api.Response, error) {
	resp, err := s.QueryNoGrpc(ctx, req)
	if err != nil {
		return resp, err
	}
	md := metadata.Pairs(x.DgraphCostHeader, fmt.Sprint(resp.Metrics.NumUids["_total"]))
	if err := grpc.SendHeader(ctx, md); err != nil {
		glog.Warningf("error in sending grpc headers: %v", err)
	}
	return resp, nil
}

// Query handles queries or mutations ，Query处理查询或突变
// 下面这个应该是后来新增的，用于处理DQL查询以及突变的，上面的那个QueryGraphQL是老版本处理GraphQL的
// ratel的普通json格式query会到这里，req对象里面有请求体
func (s *Server) QueryNoGrpc(ctx context.Context, req *api.Request) (*api.Response, error) { //NOTE:核心函数
	ctx = x.AttachJWTNamespace(ctx)

	// 判断是否开启企业ACL功能
	if x.WorkerConfig.AclEnabled && req.GetStartTs() != 0 { 
		// A fresh StartTs is assigned if it is 0.
		// 如果StartTs为0，则分配一个新的StartTs。
		ns, err := x.ExtractNamespace(ctx)
		if err != nil {
			return nil, err
		}
		if req.GetHash() != getHash(ns, req.GetStartTs()) {
			return nil, x.ErrHashMismatch
		}
	}

	// Add a timeout for queries which don't have a deadline set. We don't want to
	// apply a timeout if it's a mutation, that's currently handled by flag
	// "txn-abort-after".
	// 为没有设置截止日期的查询添加超时。如果是突变，我们不想应用超时，目前由标志“txn-abort-after”处理。
	if req.GetMutations() == nil && x.Config.QueryTimeout != 0 { // 如果非突变，且超时时间设置的非0
		// 已判定为查询请求，且全局配置了查询超时时间
		if d, _ := ctx.Deadline(); d.IsZero() { 
			// 判断出当前请求没有设置超时时间，给其设置上
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, x.Config.QueryTimeout)
			defer cancel()
		}
	}
	return s.doQuery(ctx, &Request{req: req, doAuth: getAuthMode(ctx)}) //NOTE:核心操作，查询与突变最终都会进这里面
}

func (s *Server) QueryNoAuth(ctx context.Context, req *api.Request) (*api.Response, error) {
	return s.doQuery(ctx, &Request{req: req, doAuth: NoAuthorize})
}

var pendingQueries int64
var maxPendingQueries int64
var serverOverloadErr = errors.New("429 Too Many Requests. Please throttle your requests")

func Init() {
	maxPendingQueries = x.Config.Limit.GetInt64("max-pending-queries") //设置最大等待查询数
}

// NOTE:下面函数是处理DQL查询以及突变的核心函数
func (s *Server) doQuery(ctx context.Context, req *Request) (resp *api.Response, rerr error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	defer atomic.AddInt64(&pendingQueries, -1)
	if val := atomic.AddInt64(&pendingQueries, 1); val > maxPendingQueries {
		return nil, serverOverloadErr
	}

	isGraphQL, _ := ctx.Value(IsGraphql).(bool)
	if isGraphQL { //判断是GraphQL还是DQL，统计请求的数量
		atomic.AddUint64(&numGraphQL, 1)
	} else {
		atomic.AddUint64(&numDQL, 1)
	}
	l := &query.Latency{}
	l.Start = time.Now()

	// TODO: Following trace messages have been commented out as stringified trace messages allocate
	// too much memory. These trace messages need to be generated if tracing is enabled.
	// if bool(glog.V(3)) || worker.LogDQLRequestEnabled() {
	// 	glog.Infof("Got a query, DQL form: %+v at %+v", req.req, l.Start.Format(time.RFC3339))
	// }

	isMutation := len(req.req.Mutations) > 0 //判断是否是突变
	methodRequest := methodQuery // methodRequest是一个字符串，记录是突变还是查询
	if isMutation {
		methodRequest = methodMutate
	}

	var measurements []ostats.Measurement  // Measurement是记录统计数据时测量的数值
	ctx, span := otrace.StartSpan(ctx, methodRequest) // otrace包‌主要用于跟踪和调试Go程序中的函数调用。通过在代码中插入特定的跟踪点，otrace包可以帮助开发者监控函数的调用顺序、参数和返回值
	if ns, err := x.ExtractNamespace(ctx); err == nil {  // 提取出来请求的namespace，这个变量是标识用户的（每个用户都有自己namespace），把数据进行逻辑隔离，使得每个用户只能看到属于自己的数据
		annotateNamespace(span, ns)
	}

	ctx = x.WithMethod(ctx, methodRequest) //给当前的请求上下文对象添加是查询还是突变的标识
	defer func() {
		span.End()
		v := x.TagValueStatusOK
		if rerr != nil {
			v = x.TagValueStatusError
		}
		ctx, _ = tag.New(ctx, tag.Upsert(x.KeyStatus, v))
		timeSpentMs := x.SinceMs(l.Start)
		measurements = append(measurements, x.LatencyMs.M(timeSpentMs))
		ostats.Record(ctx, measurements...)
	}()

	if rerr = x.HealthCheck(); rerr != nil { // 判断当前服务器是否允许进行请求操作
		return
	}

	req.req.Query = strings.TrimSpace(req.req.Query) // TrimSpace返回字符串s的一个片段，删除所有前导和尾随空格，如Unicode所定义。
	isQuery := len(req.req.Query) != 0
	if !isQuery && !isMutation { // 非查询也非突变，抛出错误
		span.Annotate(nil, "empty request")
		return nil, errors.Errorf("empty request")
	}

	// 下面这一块都是做数据统计的
	span.AddAttributes(otrace.StringAttribute("Query", req.req.Query))
	span.Annotatef(nil, "Request received: %v", req.req)
	if isQuery {
		//Record同时记录具有相同上下文的一个或多个测量值。
		//如果上下文中有任何标签，测量值将被标记。
		ostats.Record(ctx, x.PendingQueries.M(1), x.NumQueries.M(1)) //Measure是记录统计数据时测量的数值。
		defer func() {
			measurements = append(measurements, x.PendingQueries.M(-1))
		}()
	}
	if isMutation {
		ostats.Record(ctx, x.NumMutations.M(1))
	}

	if req.doAuth == NeedAuthorize && x.IsGalaxyOperation(ctx) {
		// Only the guardian of the galaxy can do a galaxy wide query/mutation. This operation is
		// needed by live loader.
		// 只有银河系的守护者才能进行全星系的查询/突变。活装载机需要此操作。666
		if err := AuthGuardianOfTheGalaxy(ctx); err != nil {
			s := status.Convert(err)
			return nil, status.Error(s.Code(),
				"Non guardian of galaxy user cannot bypass namespaces. "+s.Message())
		}
	}

	qc := &queryContext{ // queryContext, 它是查询上下文, 用于传递处理查询、突变或 upsert 请求所需的所有变量
		req:      req.req,
		latency:  l,
		span:     span,
		graphql:  isGraphQL,
		gqlField: req.gqlField,
	}
	// parseRequest 里又转而调用了 validateQuery 函数
	// (parseRequest, validateQuery)都只是在查询上下文的分析和验证上下功夫
	if rerr = parseRequest(ctx, qc); rerr != nil {
		return
	}

	//下面这个if进行授权判断
	if req.doAuth == NeedAuthorize {
		if rerr = authorizeRequest(ctx, qc); rerr != nil {
			return
		}
	}

	// We use defer here because for queries, startTs will be
	// assigned in the processQuery function called below.
	// 我们在这里使用defer是因为对于查询，startTs将在下面调用的processQuery函数中分配。
	defer annotateStartTs(qc.span, qc.req.StartTs)
	// For mutations, we update the startTs if necessary.
	// 对于突变，如有必要，我们会更新startTs。
	if isMutation && req.req.StartTs == 0 {
		start := time.Now()
		req.req.StartTs = worker.State.GetTimestamp(false) // 得到一个突变处理的开始时间戳
		qc.latency.AssignTimestamp = time.Since(start) // Since返回自t以来经过的时间，记录获取时间戳所花费的时间
	}
	if x.WorkerConfig.AclEnabled {
		// 如果acl（访问验证）启用
		ns, err := x.ExtractNamespace(ctx)
		if err != nil {
			return nil, err
		}
		defer func() {
			if resp != nil && resp.Txn != nil {
				// attach the hash, user must send this hash when further operating on this startTs.
				resp.Txn.Hash = getHash(ns, resp.Txn.StartTs)
			}
		}()
	}

	var gqlErrs error
	// 下面这个if结束后，就已经获取到数据了
	if resp, rerr = processQuery(ctx, qc); rerr != nil { //NOTE:核心操作，进一步完善和填充queryContext结构体，先是构造 query.Request 结构, 逐步填充这个结构, 然后调用实际业务逻辑的实施者query.Request
		// if rerr is just some error from GraphQL encoding, then we need to continue the normal
		// execution ignoring the error as we still need to assign latency info to resp. If we can
		// change the api.Response proto to have a field to contain GraphQL errors, that would be
		// great. Otherwise, we will have to do such checks a lot and that would make code ugly.
		// 如果rerr只是GraphQL编码的一些错误，那么我们需要忽略错误继续正常执行，因为我们仍然需要为resp分配延迟信息。
		// 如果我们可以更改api。响应原型有一个包含GraphQL错误的字段，那就太好了。否则，我们将不得不做很多这样的检查，这会使代码变得丑陋。
		if qc.gqlField != nil && x.IsGqlErrorList(rerr) {
			gqlErrs = rerr
		} else {
			return // 有报错，就立即返回
		}
	}
	// if it were a mutation, simple or upsert, in any case gqlErrs would be empty as GraphQL JSON
	// is formed only for queries. So, gqlErrs can have something only in the case of a pure query.
	// So, safe to ignore gqlErrs and not return that here.
	//如果它是一个突变，无论是简单的还是意外的，在任何情况下，gqlErrs都是空的，因为GraphQL JSON仅用于查询。因此，gqlErrs只有在纯查询的情况下才能有东西。
	//因此，可以安全地忽略gqlErrs，而不在此处返回。
	if rerr = s.doMutate(ctx, qc, resp); rerr != nil {  //NOTE:核心操作，进行突变操作
		// fmt.Println("进入mutate") 正常请求都不会进入这里
		return  // 有报错，就立即返回
	}

	// TODO(Ahsan): resp.Txn.Preds contain predicates of form gid-namespace|attr.
	// Remove the namespace from the response.
	// resp.Txn.Preds = x.ParseAttrList(resp.Txn.Preds)

	// TODO(martinmr): Include Transport as part of the latency. Need to do
	// this separately since it involves modifying the API protos.
	resp.Latency = &api.Latency{  // Latency统计各种操作所花费的时间，下面就是把查询或突变的时间添加到响应体resp中
		AssignTimestampNs: uint64(l.AssignTimestamp.Nanoseconds()), // Nanoseconds以整数纳秒计数返回持续时间。
		ParsingNs:         uint64(l.Parsing.Nanoseconds()),
		ProcessingNs:      uint64(l.Processing.Nanoseconds()),
		EncodingNs:        uint64(l.Json.Nanoseconds()),
		TotalNs:           uint64((time.Since(l.Start)).Nanoseconds()),
	}
	return resp, gqlErrs
}

// NOTE:极为重要的函数，处理DQL查询
func processQuery(ctx context.Context, qc *queryContext) (*api.Response, error) {
	resp := &api.Response{}
	if qc.req.Query == "" {
		// No query, so make the query cost 0.无查询体，设置查询花费为0
		resp.Metrics = &api.Metrics{
			NumUids: map[string]uint64{"_total": 0},
		}
		return resp, nil
	}
	if ctx.Err() != nil {
		return resp, ctx.Err()
	}
	// 先构造 query.Request 结构，后面的就是在填充这个qr
	qr := query.Request{
		Latency:  qc.latency,
		DqlQuery: &qc.dqlRes, //存的是解析sql语句后的结果
	}

	// Here we try our best effort to not contact Zero for a timestamp. If we succeed,
	// then we use the max known transaction ts value (from ProcessDelta) for a read-only query.
	// If we haven't processed any updates yet then fall back to getting TS from Zero.
	// 在这里，我们尽最大努力不联系Zero获取时间戳。如果我们成功了，
	// 然后我们使用最大已知事务ts值（来自ProcessDelta）进行只读查询。
	// 如果我们还没有处理任何更新，那么就从Zero开始获取TS。
	switch {  //这个Switch设置查询的模式，注意Annotate函数是添加带有属性的注释，属性可以为nil。
	case qc.req.BestEffort:
		qc.span.Annotate([]otrace.Attribute{otrace.BoolAttribute("be", true)}, "") // 最大努力的
	case qc.req.ReadOnly:
		qc.span.Annotate([]otrace.Attribute{otrace.BoolAttribute("ro", true)}, "") // 只读的
	default:
		qc.span.Annotate([]otrace.Attribute{otrace.BoolAttribute("no", true)}, "") // 默认
	}

	if qc.req.BestEffort { //如果为最大努力查询， 设置处在 NOTE:20254280
		// Sanity: check that request is read-only too.
		if !qc.req.ReadOnly {
			return resp, errors.Errorf("A best effort query must be read-only.") // 如果设置 最大努力 了，那么就必须是只读的
		}
		if qc.req.StartTs == 0 { // 如果还未分配开始时间戳
			qc.req.StartTs = posting.Oracle().MaxAssigned()  // 获取事物的开始时间戳（貌似只是在本地获取时间戳？）
		}
		qr.Cache = worker.NoCache // 设置事物是否使用缓存
	}

	if qc.req.StartTs == 0 { // 如果依旧还未分配开始时间戳
		assignTimestampStart := time.Now() // 记录请求时间戳的时间
		qc.req.StartTs = worker.State.GetTimestamp(qc.req.ReadOnly) // 异步的去获取时间戳了（去请求Zero主机获取）
		qc.latency.AssignTimestamp = time.Since(assignTimestampStart) // 记录获取时间戳所花费的时间
	}

	qr.ReadTs = qc.req.StartTs
	resp.Txn = &api.TxnContext{StartTs: qc.req.StartTs}

	// Core processing happens here.
	er, err := qr.Process(ctx) //NOTE:核心操作，调用实际业务逻辑的实施者，此行结束就获取到结果了，但是如果要客户端直接用，还需要在下面进行解析
	// 返回的结果er以子图嵌套的方式返回
	// 在er.Subgraphs（这个Subgraph就是qr的里面的那个Subgraph）里面，这个里面最上层应该对应的是查询体的最外层，然后里面还有chilren再表示属性等
	// 比如查有age的节点（如果该节点类型共有name属性与age属性，且目前数据库中共有两个含age属性的节点）
	// 	 Subgraphs切片里第一个元素就是指 查询体最外层 对应有age的节点
	// 	 然后在Subgraph.Children的每个对应一个属性，即对于本次查询，共有两个 name 与 age
	// 	 再在name与age中，各自分别有两个属性值，存储在valueMatrix切片中
	if bool(glog.V(3)) || worker.LogDQLRequestEnabled() { // 判断是否需要记录DQL请求
		glog.Infof("Finished a query that started at: %+v",
			qr.Latency.Start.Format(time.RFC3339))
	}

	if err != nil { // 做防卫式设计
		if bool(glog.V(3)) {
			glog.Infof("Error processing query: %+v\n", err.Error())
		}
		return resp, errors.Wrap(err, "")
	}

	// 下面这个if-else主要的功能就是解析er，并得到最终在客户端会用到的数据结构
	if len(er.SchemaNode) > 0 || len(er.Types) > 0 { // 处理er中Schema自身的的Predicates与Types，并给响应的结果体添加有关这两项的数据
		if err = authorizeSchemaQuery(ctx, &er); err != nil {
			return resp, err
		}
		sort.Slice(er.SchemaNode, func(i, j int) bool { // 排序Predicates
			return er.SchemaNode[i].Predicate < er.SchemaNode[j].Predicate
		})
		sort.Slice(er.Types, func(i, j int) bool { // 排序Type
			return er.Types[i].TypeName < er.Types[j].TypeName
		})

		respMap := make(map[string]interface{})
		if len(er.SchemaNode) > 0 {
			respMap["schema"] = er.SchemaNode 
		}
		if len(er.Types) > 0 {
			respMap["types"] = formatTypes(er.Types)
		}
		resp.Json, err = json.Marshal(respMap)
	} else if qc.req.RespFormat == api.Request_RDF { // 如果为为RDF请求，返回体中设置Rdf属性（依照er的Subgraphs）
		resp.Rdf, err = query.ToRDF(qc.latency, er.Subgraphs) // NOTE:核心操作，解析结果，并最终将结果放到resp.Rdf中
	} else {
		resp.Json, err = query.ToJson(ctx, qc.latency, er.Subgraphs, qc.gqlField) // NOTE:核心操作，解析结果，并最终将结果放到reso.Json中，zzlTODO:这个也可以看一下，看看怎么解析的
	}
	// if err is just some error from GraphQL encoding, then we need to continue the normal
	// execution ignoring the error as we still need to assign metrics and latency info to resp.
	if err != nil && (qc.gqlField == nil || !x.IsGqlErrorList(err)) {
		return resp, err
	}
	qc.span.Annotatef(nil, "Response = %s", resp.Json)

	// varToUID contains a map of variable name to the uids corresponding to it.
	// It is used later for constructing set and delete mutations by replacing
	// variables with the actual uids they correspond to.
	// If a variable doesn't have any UID, we generate one ourselves later.
	// varToUID包含变量名到与其对应的uid的映射。
	// 它稍后用于构建集合，并通过将变量替换为它们对应的实际uid来删除突变。
	// 如果一个变量没有任何UID，我们稍后会自己生成一个。
	for name := range qc.uidRes {
		v := qr.Vars[name]

		// If the list of UIDs is empty but the map of values is not,
		// we need to get the UIDs from the keys in the map.
		var uidList []uint64
		if v.Uids != nil && len(v.Uids.Uids) > 0 {
			uidList = v.Uids.Uids
		} else {
			uidList = make([]uint64, 0, len(v.Vals))
			for uid := range v.Vals {
				uidList = append(uidList, uid)
			}
		}
		if len(uidList) == 0 {
			continue
		}

		// We support maximum 1 million UIDs per variable to ensure that we
		// don't do bad things to alpha and mutation doesn't become too big.
		if len(uidList) > 1e6 {
			return resp, errors.Errorf("var [%v] has over million UIDs", name)
		}

		uids := make([]string, len(uidList))
		for i, u := range uidList {
			// We use base 10 here because the RDF mutations expect the uid to be in base 10.
			uids[i] = strconv.FormatUint(u, 10)
		}
		qc.uidRes[name] = uids
	}

	// look for values for value variables
	// 查找值变量的值
	for name := range qc.valRes {
		v := qr.Vars[name]
		qc.valRes[name] = v.Vals
	}

	if err := verifyUnique(qc, qr); err != nil {
		return resp, err
	}

	resp.Metrics = &api.Metrics{
		NumUids: er.Metrics,
	}
	var total uint64
	for _, num := range resp.Metrics.NumUids { // 遍历并累加 返回体 内记录的各个属性返回的数量
		total += num
	}
	resp.Metrics.NumUids["_total"] = total //Metrics包含与查询相关的所有度量。这行是把本次查询返回的属性总数记录起来

	return resp, err
}

// parseRequest parses the incoming request
func parseRequest(ctx context.Context, qc *queryContext) error {
	start := time.Now()
	defer func() {
		qc.latency.Parsing = time.Since(start)
	}()

	var needVars []string
	upsertQuery := qc.req.Query
	if len(qc.req.Mutations) > 0 {
		// parsing mutations
		qc.gmuList = make([]*dql.Mutation, 0, len(qc.req.Mutations))
		for _, mu := range qc.req.Mutations {
			gmu, err := ParseMutationObject(mu, qc.graphql)
			if err != nil {
				return err
			}

			qc.gmuList = append(qc.gmuList, gmu)
		}

		if err := addQueryIfUnique(ctx, qc); err != nil {
			return err
		}

		qc.uidRes = make(map[string][]string)
		qc.valRes = make(map[string]map[uint64]types.Val)
		upsertQuery = buildUpsertQuery(qc)
		needVars = findMutationVars(qc)
		if upsertQuery == "" {
			if len(needVars) > 0 {
				return errors.Errorf("variables %v not defined", needVars)
			}

			return nil
		}
	}

	// parsing the updated query
	var err error
	qc.dqlRes, err = dql.ParseWithNeedVars(dql.Request{
		Str:       upsertQuery,
		Variables: qc.req.Vars,
	}, needVars)
	if err != nil {
		return err
	}
	return validateQuery(qc.dqlRes.Query)
}

// verifyUnique verifies uniqueness of mutation
func verifyUnique(qc *queryContext, qr query.Request) error {
	if len(qc.uniqueVars) == 0 {
		return nil
	}

	for i, queryVar := range qc.uniqueVars {
		gmuIndex, rdfIndex := decodeIndex(i)
		pred := qc.gmuList[gmuIndex].Set[rdfIndex]
		queryResult := qr.Vars[queryVar.queryVar]
		if !isUpsertCondTrue(qc, int(gmuIndex)) {
			continue
		}
		isEmpty := func(l *pb.List) bool {
			return l == nil || len(l.Uids) == 0
		}

		var subjectUid uint64
		if strings.HasPrefix(pred.Subject, "uid(") {
			varName := qr.Vars[pred.Subject[4:len(pred.Subject)-1]]
			if isEmpty(varName.Uids) {
				subjectUid = 0 // blank node
			} else if len(varName.Uids.Uids) == 1 {
				subjectUid = varName.Uids.Uids[0]
			} else {
				return errors.Errorf("unique constraint violated for predicate [%v]", pred.Predicate)
			}
		} else {
			var err error
			subjectUid, err = parseSubject(pred.Subject)
			if err != nil {
				return errors.Wrapf(err, "error while parsing [%v]", pred.Subject)
			}
		}

		var predValue interface{}
		if strings.HasPrefix(pred.ObjectId, "val(") {
			varName := qr.Vars[pred.ObjectId[4:len(pred.ObjectId)-1]]
			val, ok := varName.Vals[0]
			if !ok {
				_, isValueGoingtoSet := varName.Vals[subjectUid]
				if !isValueGoingtoSet {
					continue
				}

				results := qr.Vars[queryVar.valVar]
				for uidOfv, v := range results.Vals {
					if v.Value == varName.Vals[subjectUid].Value && uidOfv != subjectUid {
						return errors.Errorf("could not insert duplicate value [%v] for predicate [%v]",
							v.Value, pred.Predicate)
					}
				}
				continue
			} else {
				predValue = val.Value
			}
		} else {
			predValue = dql.TypeValFrom(pred.ObjectValue).Value
		}

		// Here, we check the uniqueness of the triple by comparing the result of the uniqueQuery with the triple.
		if !isEmpty(queryResult.Uids) {
			if len(queryResult.Uids.Uids) > 1 {
				glog.Errorf("unique constraint violated for predicate [%v].uids: [%v].namespace: [%v]",
					pred.Predicate, queryResult.Uids.Uids, pred.Namespace)
				return errors.Errorf("there are duplicates in existing data for predicate [%v]."+
					"Please drop the unique constraint and re-add it after fixing the predicate data", pred.Predicate)
			} else if queryResult.Uids.Uids[0] != subjectUid {
				// Determine whether the mutation is a swap mutation
				isSwap, err := isSwap(qc, queryResult.Uids.Uids[0], pred.Predicate)
				if err != nil {
					return err
				}
				if !isSwap {
					return errors.Errorf("could not insert duplicate value [%v] for predicate [%v]",
						predValue, pred.Predicate)
				}
			}
		}
	}
	return nil
}

// addQueryIfUnique adds dummy queries in the request for checking whether predicate is unique in the db
func addQueryIfUnique(qctx context.Context, qc *queryContext) error {
	if len(qc.gmuList) == 0 {
		return nil
	}

	ctx := context.WithValue(qctx, schema.IsWrite, false)
	namespace, err := x.ExtractNamespace(ctx)
	if err != nil {
		// It's okay to ignore this here. If namespace is not set, it could mean either there is no
		// authorization or it's trying to be bypassed. So the namespace is either 0 or the mutation would fail.
		glog.Errorf("Error while extracting namespace, assuming default %s", err)
		namespace = 0
	}
	isGalaxyQuery := x.IsGalaxyOperation(ctx)

	qc.uniqueVars = map[uint64]uniquePredMeta{}
	for gmuIndex, gmu := range qc.gmuList {
		var buildQuery strings.Builder
		for rdfIndex, pred := range gmu.Set {
			if isGalaxyQuery {
				// The caller should make sure that the directed edges contain the namespace we want
				// to insert into.
				namespace = pred.Namespace
			}
			if pred.Predicate != "dgraph.xid" {
				// [TODO] Don't check if it's dgraph.xid. It's a bug as this node might not be aware
				// of the schema for the given predicate. This is a bug issue for dgraph.xid hence
				// we are bypassing it manually until the bug is fixed.
				predSchema, ok := schema.State().Get(ctx, x.NamespaceAttr(namespace, pred.Predicate))
				if !ok || !predSchema.Unique {
					continue
				}
			}

			// Wrapping predicateName with angle brackets ensures that if the predicate contains any non-Latin letters,
			// the unique query will not fail. Additionally,
			// it helps ensure that non-Latin predicate names are properly formatted
			// during the automatic serialization of a structure into JSON.
			predicateName := fmt.Sprintf("<%v>", pred.Predicate)
			if pred.Lang != "" {
				predicateName = fmt.Sprintf("%v@%v", predicateName, pred.Lang)
			}

			uniqueVarMapKey := encodeIndex(gmuIndex, rdfIndex)
			queryVar := fmt.Sprintf("__dgraph_uniquecheck_%v__", uniqueVarMapKey)
			// Now, we add a query for a predicate to check if the value of the
			// predicate sent in the mutation already exists in the DB.
			// For example, schema => email: string @unique @index(exact) .\
			//
			// To ensure uniqueness of values of email predicate, we will query for the value
			// to ensure that we are not adding a duplicate value.
			//
			// There are following use-cases of mutations which we need to handle:
			//   1. _:a <email> "example@email.com"  .
			//   2. _:a <email> val(queryVariable)  .
			// In the code below, we construct respective query for the above use-cases viz.
			//   __dgraph_uniquecheck_1__ as var(func: eq(email,"example@email.com"))
			//   __dgraph_uniquecheck_2__ as var(func: eq(email, val(queryVariable))){
			//       uid
			//       __dgraph_uniquecheck_val_2__ as email
			//   }
			// Each of the above query will check if there is already an existing value.
			// We can be sure that we may get at most one UID in return.
			// If the returned UID is different than the UID we have
			// in the mutation, then we reject the mutation.

			if !strings.HasPrefix(pred.ObjectId, "val(") {
				query := fmt.Sprintf(`%v as var(func: eq(%v,"%v"))`, queryVar, predicateName,
					dql.TypeValFrom(pred.ObjectValue).Value)
				if _, err := buildQuery.WriteString(query); err != nil {
					return errors.Wrapf(err, "error while writing string")
				}
				qc.uniqueVars[uniqueVarMapKey] = uniquePredMeta{queryVar: queryVar}
			} else {
				valQueryVar := fmt.Sprintf("__dgraph_uniquecheck_val_%v__", uniqueVarMapKey)
				query := fmt.Sprintf(`%v as var(func: eq(%v,%v)){
					                             uid
					                             %v as %v
				                 }`, queryVar, predicateName, pred.ObjectId, valQueryVar, predicateName)
				if _, err := buildQuery.WriteString(query); err != nil {
					return errors.Wrapf(err, "error while building unique query")
				}
				qc.uniqueVars[uniqueVarMapKey] = uniquePredMeta{valVar: valQueryVar, queryVar: queryVar}
			}
		}

		if buildQuery.Len() > 0 {
			if _, err := buildQuery.WriteString("}"); err != nil {
				return errors.Wrapf(err, "error while writing string")
			}
			if qc.req.Query == "" {
				qc.req.Query = "{" + buildQuery.String()
			} else {
				qc.req.Query = strings.TrimSuffix(qc.req.Query, "}")
				qc.req.Query = qc.req.Query + buildQuery.String()
			}
		}
	}
	return nil
}

func authorizeRequest(ctx context.Context, qc *queryContext) error {
	if err := authorizeQuery(ctx, &qc.dqlRes, qc.graphql); err != nil {
		return err
	}

	// TODO(Aman): can be optimized to do the authorization in just one func call
	for _, gmu := range qc.gmuList {
		if err := authorizeMutation(ctx, gmu); err != nil {
			return err
		}
	}

	return nil
}

func getHash(ns, startTs uint64) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%#x%#x%#x", ns, startTs, []byte(worker.Config.AclSecretKeyBytes))))
	return hex.EncodeToString(h.Sum(nil))
}

func validateNamespace(ctx context.Context, tc *api.TxnContext) error {
	if !x.WorkerConfig.AclEnabled {
		return nil
	}

	ns, err := x.ExtractNamespaceFrom(ctx)
	if err != nil {
		return err
	}
	if tc.Hash != getHash(ns, tc.StartTs) {
		return x.ErrHashMismatch
	}
	return nil
}

// CommitOrAbort commits or aborts a transaction.
func (s *Server) CommitOrAbort(ctx context.Context, tc *api.TxnContext) (*api.TxnContext, error) {
	ctx, span := otrace.StartSpan(ctx, "Server.CommitOrAbort")
	defer span.End()

	if err := x.HealthCheck(); err != nil {
		return &api.TxnContext{}, err
	}

	tctx := &api.TxnContext{}
	if tc.StartTs == 0 {
		return &api.TxnContext{}, errors.Errorf(
			"StartTs cannot be zero while committing a transaction")
	}
	if ns, err := x.ExtractNamespaceFrom(ctx); err == nil {
		annotateNamespace(span, ns)
	}
	annotateStartTs(span, tc.StartTs)

	if err := validateNamespace(ctx, tc); err != nil {
		return &api.TxnContext{}, err
	}

	span.Annotatef(nil, "Txn Context received: %+v", tc)
	commitTs, err := worker.CommitOverNetwork(ctx, tc)
	if err == dgo.ErrAborted {
		// If err returned is dgo.ErrAborted and tc.Aborted was set, that means the client has
		// aborted the transaction by calling txn.Discard(). Hence return a nil error.
		tctx.Aborted = true
		if tc.Aborted {
			return tctx, nil
		}

		return tctx, status.Errorf(codes.Aborted, err.Error())
	}
	tctx.StartTs = tc.StartTs
	tctx.CommitTs = commitTs
	return tctx, err
}

// CheckVersion returns the version of this Dgraph instance.
func (s *Server) CheckVersion(ctx context.Context, c *api.Check) (v *api.Version, err error) {
	if err := x.HealthCheck(); err != nil {
		return v, err
	}

	v = new(api.Version)
	v.Tag = x.Version()
	return v, nil
}

// -------------------------------------------------------------------------------------------------
// HELPER FUNCTIONS
// -------------------------------------------------------------------------------------------------
func isMutationAllowed(ctx context.Context) bool {
	if worker.Config.MutationsMode != worker.DisallowMutations {
		return true
	}
	shareAllowed, ok := ctx.Value("_share_").(bool)
	if !ok || !shareAllowed {
		return false
	}
	return true
}

var errNoAuth = errors.Errorf("No Auth Token found. Token needed for Admin operations.")

func hasAdminAuth(ctx context.Context, tag string) (net.Addr, error) {
	ipAddr, err := x.HasWhitelistedIP(ctx)
	if err != nil {
		return nil, err
	}
	glog.Infof("Got %s request from: %q\n", tag, ipAddr)
	if err = hasPoormansAuth(ctx); err != nil {
		return nil, err
	}
	return ipAddr, nil
}

func hasPoormansAuth(ctx context.Context) error {
	if worker.Config.AuthToken == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return errNoAuth
	}
	tokens := md.Get("auth-token")
	if len(tokens) == 0 {
		return errNoAuth
	}
	if tokens[0] != worker.Config.AuthToken {
		return errors.Errorf("Provided auth token [%s] does not match. Permission denied.", tokens[0])
	}
	return nil
}

// ParseMutationObject tries to consolidate fields of the api.Mutation into the
// corresponding field of the returned dql.Mutation. For example, the 3 fields,
// api.Mutation#SetJson, api.Mutation#SetNquads and api.Mutation#Set are consolidated into the
// dql.Mutation.Set field. Similarly the 3 fields api.Mutation#DeleteJson, api.Mutation#DelNquads
// and api.Mutation#Del are merged into the dql.Mutation#Del field.
func ParseMutationObject(mu *api.Mutation, isGraphql bool) (*dql.Mutation, error) {
	res := &dql.Mutation{Cond: mu.Cond}

	if len(mu.SetJson) > 0 {
		nqs, md, err := chunker.ParseJSON(mu.SetJson, chunker.SetNquads)
		if err != nil {
			return nil, err
		}
		res.Set = append(res.Set, nqs...)
		res.Metadata = md
	}
	if len(mu.DeleteJson) > 0 {
		// The metadata is not currently needed for delete operations so it can be safely ignored.
		nqs, _, err := chunker.ParseJSON(mu.DeleteJson, chunker.DeleteNquads)
		if err != nil {
			return nil, err
		}
		res.Del = append(res.Del, nqs...)
	}
	if len(mu.SetNquads) > 0 {
		nqs, md, err := chunker.ParseRDFs(mu.SetNquads)
		if err != nil {
			return nil, err
		}
		res.Set = append(res.Set, nqs...)
		res.Metadata = md
	}
	if len(mu.DelNquads) > 0 {
		nqs, _, err := chunker.ParseRDFs(mu.DelNquads)
		if err != nil {
			return nil, err
		}
		res.Del = append(res.Del, nqs...)
	}

	res.Set = append(res.Set, mu.Set...)
	res.Del = append(res.Del, mu.Del...)
	// parse facets and convert to the binary format so that
	// a field of type datetime like "2017-01-01" can be correctly encoded in the
	// marshaled binary format as done in the time.Marshal method
	if err := validateAndConvertFacets(res.Set); err != nil {
		return nil, err
	}

	if err := validateNQuads(res.Set, res.Del, isGraphql); err != nil {
		return nil, err
	}
	return res, nil
}

func validateAndConvertFacets(nquads []*api.NQuad) error {
	for _, m := range nquads {
		encodedFacets := make([]*api.Facet, 0, len(m.Facets))
		for _, f := range m.Facets {
			// try to interpret the value as binary first
			if _, err := facets.ValFor(f); err == nil {
				encodedFacets = append(encodedFacets, f)
			} else {
				encodedFacet, err := facets.FacetFor(f.Key, string(f.Value))
				if err != nil {
					return err
				}
				encodedFacets = append(encodedFacets, encodedFacet)
			}
		}

		m.Facets = encodedFacets
	}
	return nil
}

// validateForGraphql validate nquads for graphql
func validateForGraphql(nq *api.NQuad, isGraphql bool) error {
	// Check whether the incoming predicate is graphql reserved predicate or not.
	if !isGraphql && x.IsGraphqlReservedPredicate(nq.Predicate) {
		return errors.Errorf("Cannot mutate graphql reserved predicate %s", nq.Predicate)
	}
	return nil
}

func validateNQuads(set, del []*api.NQuad, isGraphql bool) error {
	for _, nq := range set {
		if err := validatePredName(nq.Predicate); err != nil {
			return err
		}
		var ostar bool
		if o, ok := nq.ObjectValue.GetVal().(*api.Value_DefaultVal); ok {
			ostar = o.DefaultVal == x.Star
		}
		if nq.Subject == x.Star || nq.Predicate == x.Star || ostar {
			return errors.Errorf("Cannot use star in set n-quad: %+v", nq)
		}
		if err := validateKeys(nq); err != nil {
			return errors.Wrapf(err, "key error: %+v", nq)
		}
		if err := validateForGraphql(nq, isGraphql); err != nil {
			return err
		}
	}
	for _, nq := range del {
		if err := validatePredName(nq.Predicate); err != nil {
			return err
		}
		var ostar bool
		if o, ok := nq.ObjectValue.GetVal().(*api.Value_DefaultVal); ok {
			ostar = o.DefaultVal == x.Star
		}
		if nq.Subject == x.Star || (nq.Predicate == x.Star && !ostar) {
			return errors.Errorf("Only valid wildcard delete patterns are 'S * *' and 'S P *': %v", nq)
		}
		if err := validateForGraphql(nq, isGraphql); err != nil {
			return err
		}
		// NOTE: we dont validateKeys() with delete to let users fix existing mistakes
		// with bad predicate forms. ex: foo@bar ~something
	}
	return nil
}

func validateKey(key string) error {
	switch {
	case key == "":
		return errors.Errorf("Has zero length")
	case strings.ContainsAny(key, "~@"):
		return errors.Errorf("Has invalid characters")
	case strings.IndexFunc(key, unicode.IsSpace) != -1:
		return errors.Errorf("Must not contain spaces")
	}
	return nil
}

// validateKeys checks predicate and facet keys in N-Quad for syntax errors.
func validateKeys(nq *api.NQuad) error {
	if err := validateKey(nq.Predicate); err != nil {
		return errors.Wrapf(err, "predicate %q", nq.Predicate)
	}
	for i := range nq.Facets {
		if nq.Facets[i] == nil {
			continue
		}
		if err := validateKey(nq.Facets[i].Key); err != nil {
			return errors.Errorf("Facet %q, %s", nq.Facets[i].Key, err)
		}
	}
	return nil
}

// validateQuery verifies that the query does not contain any preds that
// are longer than the limit (2^16).
func validateQuery(queries []*dql.GraphQuery) error {
	for _, q := range queries {
		if err := validatePredName(q.Attr); err != nil {
			return err
		}

		if err := validateQuery(q.Children); err != nil {
			return err
		}
	}

	return nil
}

func validatePredName(name string) error {
	if len(name) > math.MaxUint16 {
		return errors.Errorf("Predicate name length cannot be bigger than 2^16. Predicate: %v",
			name[:80])
	}
	return nil
}

// formatTypes takes a list of TypeUpdates and converts them in to a list of
// maps in a format that is human-readable to be marshaled into JSON.
func formatTypes(typeList []*pb.TypeUpdate) []map[string]interface{} {
	var res []map[string]interface{}
	for _, typ := range typeList {
		typeMap := make(map[string]interface{})
		typeMap["name"] = typ.TypeName
		fields := make([]map[string]string, len(typ.Fields))

		for i, field := range typ.Fields {
			m := make(map[string]string, 1)
			m["name"] = field.Predicate
			fields[i] = m
		}
		typeMap["fields"] = fields

		res = append(res, typeMap)
	}
	return res
}

func isDropAll(op *api.Operation) bool {
	if op.DropAll || op.DropOp == api.Operation_ALL {
		return true
	}
	return false
}

func verifyUniqueWithinMutation(qc *queryContext) error {
	if len(qc.uniqueVars) == 0 {
		return nil
	}

	for i := range qc.uniqueVars {
		gmuIndex, rdfIndex := decodeIndex(i)
		if len(qc.gmuList[gmuIndex].Set) == 0 {
			return nil
		}
		pred1 := qc.gmuList[gmuIndex].Set[rdfIndex]
		pred1Value := dql.TypeValFrom(pred1.ObjectValue).Value
		for j := range qc.uniqueVars {
			gmuIndex2, rdfIndex2 := decodeIndex(j)
			pred2 := qc.gmuList[gmuIndex2].Set[rdfIndex2]
			if pred2.Predicate == pred1.Predicate && dql.TypeValFrom(pred2.ObjectValue).Value == pred1Value &&
				pred2.Subject != pred1.Subject {
				return errors.Errorf("could not insert duplicate value [%v] for predicate [%v]",
					dql.TypeValFrom(pred1.ObjectValue).Value, pred1.Predicate)
			}
		}
	}
	return nil
}

func isUpsertCondTrue(qc *queryContext, gmuIndex int) bool {
	condVar := qc.condVars[gmuIndex]
	if condVar == "" {
		return true
	}

	uids, ok := qc.uidRes[condVar]
	return ok && len(uids) == 1
}

func isSwap(qc *queryContext, pred1SubjectUid uint64, pred1Predicate string) (bool, error) {
	for i := range qc.uniqueVars {
		gmuIndex, rdfIndex := decodeIndex(i)
		pred2 := qc.gmuList[gmuIndex].Set[rdfIndex]
		var pred2SubjectUid uint64
		if !strings.HasPrefix(pred2.Subject, "uid(") {
			var err error
			pred2SubjectUid, err = parseSubject(pred2.Subject)
			if err != nil {
				return false, errors.Wrapf(err, "error while parsing [%v]", pred2.Subject)
			}
		} else {
			pred2SubjectUid = 0
		}

		if pred2SubjectUid == pred1SubjectUid && pred1Predicate == pred2.Predicate {
			return true, nil
		}
	}
	return false, nil
}

// encodeBit two uint32 numbers by bit.
// First 32 bits store k1 and last 32 bits store k2.
func encodeIndex(k1, k2 int) uint64 {
	safeToConvert := func(num int) {
		if num > math.MaxUint32 {
			panic("unsafe conversion: integer value is too large for uint32")
		}
	}
	safeToConvert(k1)
	safeToConvert(k2)
	return uint64(uint32(k1))<<32 | uint64(uint32(k2))
}

func decodeIndex(pair uint64) (uint32, uint32) {
	k1 := uint32(pair >> 32)
	k2 := uint32(pair) & 0xFFFFFFFF
	return k1, k2
}

func parseSubject(predSubject string) (uint64, error) {
	if strings.HasPrefix(predSubject, "_:") {
		return 0, nil // blank node
	} else {
		return dql.ParseUid(predSubject)
	}
}
