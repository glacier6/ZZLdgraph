/*
 * Copyright 2016-2025 Hypermode Inc. and Contributors
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

package worker

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	cindex "github.com/google/codesearch/index"
	cregexp "github.com/google/codesearch/regexp"
	"github.com/pkg/errors"
	otrace "go.opencensus.io/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/hypermodeinc/dgraph/v24/algo"
	"github.com/hypermodeinc/dgraph/v24/conn"
	"github.com/hypermodeinc/dgraph/v24/posting"
	"github.com/hypermodeinc/dgraph/v24/protos/pb"
	"github.com/hypermodeinc/dgraph/v24/schema"
	ctask "github.com/hypermodeinc/dgraph/v24/task"
	"github.com/hypermodeinc/dgraph/v24/tok"
	"github.com/hypermodeinc/dgraph/v24/tok/hnsw"
	"github.com/hypermodeinc/dgraph/v24/tok/index"
	"github.com/hypermodeinc/dgraph/v24/types"
	"github.com/hypermodeinc/dgraph/v24/types/facets"
	"github.com/hypermodeinc/dgraph/v24/x"
)

// 调用到另一个Dgraph Alpha实例上 的 ServeTask 接口
func invokeNetworkRequest(ctx context.Context, addr string,
	f func(context.Context, pb.WorkerClient) (interface{}, error)) (interface{}, error) {
	pl, err := conn.GetPools().Get(addr)
	if err != nil {
		return nil, errors.Wrapf(err, "dispatchTaskOverNetwork: while retrieving connection.")
	}

	if span := otrace.FromContext(ctx); span != nil {
		span.Annotatef(nil, "invokeNetworkRequest: Sending request to %v", addr)
	}
	c := pb.NewWorkerClient(pl.Get())
	return f(ctx, c)
}

const backupRequestGracePeriod = time.Second

// TODO: Cross-server cancellation as described in Jeff Dean's talk.
func processWithBackupRequest(
	ctx context.Context,
	gid uint32,
	f func(context.Context, pb.WorkerClient) (interface{}, error)) (interface{}, error) {
	addrs := groups().AnyTwoServers(gid)
	if len(addrs) == 0 {
		return nil, errors.New("No network connection")
	}
	if len(addrs) == 1 {
		reply, err := invokeNetworkRequest(ctx, addrs[0], f) //NOTE:核心操作，去别的组查了
		return reply, err
	}
	type taskresult struct {
		reply interface{}
		err   error
	}

	chResults := make(chan taskresult, len(addrs))
	ctx0, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		reply, err := invokeNetworkRequest(ctx0, addrs[0], f)
		chResults <- taskresult{reply, err}
	}()

	timer := time.NewTimer(backupRequestGracePeriod)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		go func() {
			reply, err := invokeNetworkRequest(ctx0, addrs[1], f)
			chResults <- taskresult{reply, err}
		}()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-chResults:
			if result.err != nil {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case result := <-chResults:
					return result.reply, result.err
				}
			} else {
				return result.reply, nil
			}
		}
	case result := <-chResults:
		if result.err != nil {
			cancel() // Might as well cleanup resources ASAP
			timer.Stop()
			return invokeNetworkRequest(ctx, addrs[1], f)
		}
		return result.reply, nil
	}
}

// ProcessTaskOverNetwork is used to process the query and get the result from
// the instance which stores posting list corresponding to the predicate in the
// query.
// ProcessTaskOverNetwork用于处理查询，并从存储查询中谓词对应的过账列表的实例中获取结果。
// 该函数先获取当前查询所属的组 ID, 然后判断是不是在当前alpha上, 如果是则执行立即processTask, 否则发起 RPC 远程调用。
func ProcessTaskOverNetwork(ctx context.Context, q *pb.Query) (*pb.Result, error) {
	attr := q.Attr // 获取DQL中定义的标签属性（即谓词）
	gid, err := groups().BelongsToReadOnly(attr, q.ReadTs) // groups()函数获取全局的组对象，然后再调用BelongsToReadOnly得到目标谓词的所属group组ID
	switch {
	case err != nil:
		return nil, err
	case gid == 0:
		return nil, errNonExistentTablet
	}

	span := otrace.FromContext(ctx)
	if span != nil {
		span.Annotatef(nil, "ProcessTaskOverNetwork. attr: %v gid: %v, readTs: %d, node id: %d",
			attr, gid, q.ReadTs, groups().Node.Id)
	}

	if groups().ServesGroup(gid) { //如果目标谓词所在的组ID与当前组ID一样，就直接本地查询
		// No need for a network call, as this should be run from within this instance.
		// 不需要网络调用，因为这应该在此实例中运行。
		return processTask(ctx, q, gid) // NOTE:核心操作，直接本地查询
	}

	result, err := processWithBackupRequest(ctx, gid, //NOTE:核心操作，不在当前组，此时应该发起RPC远程调用 zzlTODO:待看RPC是怎么发的，又是怎么接受处理的？
		func(ctx context.Context, c pb.WorkerClient) (interface{}, error) {
			return c.ServeTask(ctx, q)
		})
	if err != nil {
		return nil, err
	}

	reply := result.(*pb.Result)
	if span != nil {
		span.Annotatef(nil, "Reply from server. len: %v gid: %v Attr: %v",
			len(reply.UidMatrix), gid, attr)
	}
	return reply, nil
}

// convertValue converts the data to the schema.State() type of predicate.
func convertValue(attr, data string) (types.Val, error) {
	// Parse given value and get token. There should be only one token.
	t, err := schema.State().TypeOf(attr)
	if err != nil {
		return types.Val{}, err
	}
	if !t.IsScalar() {
		return types.Val{}, errors.Errorf("Attribute %s is not valid scalar type",
			x.ParseAttr(attr))
	}
	src := types.Val{Tid: types.StringID, Value: []byte(data)}
	dst, err := types.Convert(src, t)
	return dst, err
}

// Returns nil byte on error
func convertToType(v types.Val, typ types.TypeID) (*pb.TaskValue, error) {
	result := &pb.TaskValue{ValType: typ.Enum(), Val: x.Nilbyte}
	if v.Tid == typ {
		result.Val = v.Value.([]byte)
		return result, nil
	}

	// convert data from binary to appropriate format
	val, err := types.Convert(v, typ)
	if err != nil {
		return result, err
	}
	// Marshal
	data := types.ValueForType(types.BinaryID)
	err = types.Marshal(val, &data)
	if err != nil {
		return result, errors.Errorf("Failed convertToType during Marshal")
	}
	result.Val = data.Value.([]byte)
	return result, nil
}

// FuncType represents the type of a query function (aggregation, has, etc).
type FuncType int

const (
	notAFunction FuncType = iota
	aggregatorFn
	compareAttrFn
	compareScalarFn
	geoFn
	passwordFn
	regexFn
	fullTextSearchFn
	hasFn
	uidInFn
	customIndexFn
	matchFn
	similarToFn
	standardFn = 100
)

func parseFuncType(srcFunc *pb.SrcFunction) (FuncType, string) {
	if srcFunc == nil {
		return notAFunction, ""
	}
	ftype, fname := parseFuncTypeHelper(srcFunc.Name)
	if srcFunc.IsCount && ftype == compareAttrFn {
		// gt(release_date, "1990") is 'CompareAttr' which
		//    takes advantage of indexed-attr
		// gt(count(films), 0) is 'CompareScalar', we first do
		//    counting on attr, then compare the result as scalar with int
		return compareScalarFn, fname
	}
	return ftype, fname
}

func parseFuncTypeHelper(name string) (FuncType, string) {
	if len(name) == 0 {
		return notAFunction, ""
	}
	f := strings.ToLower(name)
	switch f {
	case "le", "ge", "lt", "gt", "eq", "between":
		return compareAttrFn, f
	case "min", "max", "sum", "avg":
		return aggregatorFn, f
	case "checkpwd":
		return passwordFn, f
	case "regexp":
		return regexFn, f
	case "alloftext", "anyoftext":
		return fullTextSearchFn, f
	case "has":
		return hasFn, f
	case "uid_in":
		return uidInFn, f
	case "similar_to":
		return similarToFn, f
	case "anyof", "allof":
		return customIndexFn, f
	case "match":
		return matchFn, f
	default:
		if types.IsGeoFunc(f) {
			return geoFn, f
		}
		return standardFn, f
	}
}

func needsIndex(fnType FuncType, uidList *pb.List) bool {
	switch fnType {
	case compareAttrFn:
		if uidList != nil {
			// UidList is not nil means this is a filter. Filter predicate is not indexed, so
			// instead of fetching values by index key, we will fetch value by data key
			// (from uid and predicate) and apply filter on values.
			return false
		}
		return true
	case geoFn, fullTextSearchFn, standardFn, matchFn:
		return true
	case similarToFn:
		return true
	}
	return false
}

// needsIntersect checks if the function type needs algo.IntersectSorted() after the results
// are collected. This is needed for functions that require all values to  match, like
// "allofterms", "alloftext", and custom functions with "allof".
// Returns true if function results need intersect, false otherwise.
// needsIntersect检查函数类型是否需要algo。收集结果后使用IntersectSorted（）。对于需要所有值匹配的函数，如“allofterms”、“alloftext”和带有“allof”的自定义函数，这是必需的。
// 如果函数结果需要相交，则返回true，否则返回false。
func needsIntersect(fnName string) bool {
	return strings.HasPrefix(fnName, "allof") || strings.HasSuffix(fnName, "allof")
}

type funcArgs struct {
	q     *pb.Query
	gid   uint32
	srcFn *functionContext
	out   *pb.Result
}

// The function tells us whether we want to fetch value posting lists or uid posting lists.
func (srcFn *functionContext) needsValuePostings(typ types.TypeID) (bool, error) {
	switch srcFn.fnType {
	case aggregatorFn, passwordFn, similarToFn:
		return true, nil
	case compareAttrFn:
		if len(srcFn.tokens) > 0 {
			return false, nil
		}
		return true, nil
	case geoFn, regexFn, fullTextSearchFn, standardFn, hasFn, customIndexFn, matchFn:
		// All of these require an index, hence would require fetching uid postings.
		return false, nil
	case uidInFn, compareScalarFn:
		// Operate on uid postings
		return false, nil
	case notAFunction:
		return typ.IsScalar(), nil
	}
	return false, errors.Errorf("Unhandled case in fetchValuePostings for fn: %s", srcFn.fname)
}

// Handles fetching of value posting lists and filtering of uids based on that.
func (qs *queryState) handleValuePostings(ctx context.Context, args funcArgs) error {
	srcFn := args.srcFn
	q := args.q

	facetsTree, err := preprocessFilter(q.FacetsFilter)
	if err != nil {
		return err
	}

	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, "handleValuePostings")
	defer stop()
	if span != nil {
		span.Annotatef(nil, "Number of uids: %d. args.srcFn: %+v", srcFn.n, args.srcFn)
	}

	switch srcFn.fnType {
	case notAFunction, aggregatorFn, passwordFn, compareAttrFn, similarToFn:
	default:
		return errors.Errorf("Unhandled function in handleValuePostings: %s", srcFn.fname)
	}

	if srcFn.fnType == similarToFn {
		numNeighbors, err := strconv.ParseInt(q.SrcFunc.Args[0], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid value for number of neighbors: %s", q.SrcFunc.Args[0])
		}
		cspec, err := pickFactoryCreateSpec(ctx, args.q.Attr)
		if err != nil {
			return err
		}
		//TODO: generate maxLevels from schema, filter, etc.
		qc := hnsw.NewQueryCache(
			posting.NewViLocalCache(qs.cache),
			args.q.ReadTs,
		)
		indexer, err := cspec.CreateIndex(args.q.Attr)
		if err != nil {
			return err
		}
		var nnUids []uint64
		if srcFn.vectorInfo != nil {
			nnUids, err = indexer.Search(ctx, qc, srcFn.vectorInfo,
				int(numNeighbors), index.AcceptAll[float32])
		} else {
			nnUids, err = indexer.SearchWithUid(ctx, qc, srcFn.vectorUid,
				int(numNeighbors), index.AcceptAll[float32])
		}

		if err != nil && !strings.Contains(err.Error(), hnsw.EmptyHNSWTreeError+": "+badger.ErrKeyNotFound.Error()) {
			return err
		}
		sort.Slice(nnUids, func(i, j int) bool { return nnUids[i] < nnUids[j] })
		args.out.UidMatrix = append(args.out.UidMatrix, &pb.List{Uids: nnUids})
		return nil
	}

	if srcFn.atype == types.PasswordID && srcFn.fnType != passwordFn {
		// Silently skip if the user is trying to fetch an attribute of type password.
		return nil
	}
	if srcFn.fnType == passwordFn && srcFn.atype != types.PasswordID {
		return errors.Errorf("checkpwd fn can only be used on attr: [%s] with schema type "+
			"password. Got type: %s", x.ParseAttr(q.Attr), srcFn.atype.Name())
	}
	if srcFn.n == 0 {
		return nil
	}

	// srcFn.n should be equal to len(q.UidList.Uids) for below implementation(DivideAndRule and
	// calculate) to work correctly. But we have seen some panics while forming DataKey in
	// calculate(). panic is of the form "index out of range [4] with length 1". Hence return error
	// from here when srcFn.n != len(q.UidList.Uids).
	if srcFn.n != len(q.UidList.Uids) {
		return errors.Errorf("srcFn.n: %d is not equal to len(q.UidList.Uids): %d, srcFn: %+v in "+
			"handleValuePostings", srcFn.n, len(q.UidList.GetUids()), srcFn)
	}

	// This function has small boilerplate as handleUidPostings, around how the code gets
	// concurrently executed. I didn't see much value in trying to separate it out, because the core
	// logic constitutes most of the code volume here.
	numGo, width := x.DivideAndRule(srcFn.n)
	x.AssertTrue(width > 0)
	span.Annotatef(nil, "Width: %d. NumGo: %d", width, numGo)

	outputs := make([]*pb.Result, numGo)
	listType := schema.State().IsList(q.Attr)

	// These are certain special cases where we can get away with reading only the latest value
	// Lang doesn't work because we would be storing various different languages at various
	// time. So when we go to read the latest value, we might get a different language.
	// Similarly with DoCount and ExpandAll and Facets. List types are also not supported
	// because list is stored by time, and we combine all the list items at various timestamps.
	hasLang := schema.State().HasLang(q.Attr)
	getMultiplePosting := q.DoCount || q.ExpandAll || listType || hasLang || q.FacetParam != nil

	calculate := func(start, end int) error {
		x.AssertTrue(start%width == 0)
		out := &pb.Result{}
		outputs[start/width] = out

		for i := start; i < end; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			key := x.DataKey(q.Attr, q.UidList.Uids[i])

			// Get or create the posting list for an entity, attribute combination.

			var vals []types.Val
			fcs := &pb.FacetsList{FacetsList: make([]*pb.Facets, 0)} // TODO Figure out how it is stored

			if !getMultiplePosting {
				pl, err := qs.cache.GetSinglePosting(key)
				if err != nil {
					return err
				}
				if pl == nil || len(pl.Postings) == 0 {
					out.UidMatrix = append(out.UidMatrix, &pb.List{})
					out.FacetMatrix = append(out.FacetMatrix, &pb.FacetsList{})
					out.ValueMatrix = append(out.ValueMatrix,
						&pb.ValueList{Values: []*pb.TaskValue{}})
					continue
				}
				vals = make([]types.Val, len(pl.Postings))
				for i, p := range pl.Postings {
					vals[i] = types.Val{
						Tid:   types.TypeID(p.ValType),
						Value: p.Value,
					}
				}
			} else {
				pl, err := qs.cache.Get(key)
				if err != nil {
					return err
				}

				// If count is being requested, there is no need to populate value and facets matrix.
				if q.DoCount {
					count, err := countForValuePostings(args, pl, facetsTree, listType)
					if err != nil && err != posting.ErrNoValue {
						return err
					}
					out.Counts = append(out.Counts, uint32(count))
					// Add an empty UID list to make later processing consistent.
					out.UidMatrix = append(out.UidMatrix, &pb.List{})
					continue
				}

				vals, fcs, err = retrieveValuesAndFacets(args, pl, facetsTree, listType)

				switch {
				case err == posting.ErrNoValue || (err == nil && len(vals) == 0):
					// This branch is taken when the value does not exist in the pl or
					// the number of values retrieved is zero (there could still be facets).
					// We add empty lists to the UidMatrix, FaceMatrix, ValueMatrix and
					// LangMatrix so that all these data structure have predictable layouts.
					out.UidMatrix = append(out.UidMatrix, &pb.List{})
					out.FacetMatrix = append(out.FacetMatrix, &pb.FacetsList{})
					out.ValueMatrix = append(out.ValueMatrix,
						&pb.ValueList{Values: []*pb.TaskValue{}})
					if q.ExpandAll {
						// To keep the cardinality same as that of ValueMatrix.
						out.LangMatrix = append(out.LangMatrix, &pb.LangList{})
					}
					continue
				case err != nil:
					return err
				}

				if q.ExpandAll {
					langTags, err := pl.GetLangTags(args.q.ReadTs)
					if err != nil {
						return err
					}
					out.LangMatrix = append(out.LangMatrix, &pb.LangList{Lang: langTags})
				}
			}

			uidList := new(pb.List)
			var vl pb.ValueList
			for _, val := range vals {
				newValue, err := convertToType(val, srcFn.atype)
				if err != nil {
					return err
				}

				// This means we fetched the value directly instead of fetching index key and
				// intersecting. Lets compare the value and add filter the uid.
				if srcFn.fnType == compareAttrFn {
					// Lets convert the val to its type.
					if val, err = types.Convert(val, srcFn.atype); err != nil {
						return err
					}
					switch srcFn.fname {
					case "eq":
						for _, eqToken := range srcFn.eqTokens {
							if types.CompareVals(srcFn.fname, val, eqToken) {
								uidList.Uids = append(uidList.Uids, q.UidList.Uids[i])
								break
							}
						}
					case "between":
						if types.CompareBetween(val, srcFn.eqTokens[0], srcFn.eqTokens[1]) {
							uidList.Uids = append(uidList.Uids, q.UidList.Uids[i])
						}
					default:
						if types.CompareVals(srcFn.fname, val, srcFn.eqTokens[0]) {
							uidList.Uids = append(uidList.Uids, q.UidList.Uids[i])
						}
					}

				} else {
					vl.Values = append(vl.Values, newValue)
				}
			}
			out.ValueMatrix = append(out.ValueMatrix, &vl)

			// Add facets to result.
			out.FacetMatrix = append(out.FacetMatrix, fcs)

			switch {
			case srcFn.fnType == aggregatorFn:
				// Add an empty UID list to make later processing consistent
				out.UidMatrix = append(out.UidMatrix, &pb.List{})
			case srcFn.fnType == passwordFn:
				lastPos := len(out.ValueMatrix) - 1
				if len(out.ValueMatrix[lastPos].Values) == 0 {
					continue
				}
				newValue := out.ValueMatrix[lastPos].Values[0]
				if len(newValue.Val) == 0 {
					out.ValueMatrix[lastPos].Values[0] = ctask.FalseVal
				}
				pwd := q.SrcFunc.Args[0]
				err = types.VerifyPassword(pwd, string(newValue.Val))
				if err != nil {
					out.ValueMatrix[lastPos].Values[0] = ctask.FalseVal
				} else {
					out.ValueMatrix[lastPos].Values[0] = ctask.TrueVal
				}
				// Add an empty UID list to make later processing consistent
				out.UidMatrix = append(out.UidMatrix, &pb.List{})
			default:
				out.UidMatrix = append(out.UidMatrix, uidList)
			}
		}
		return nil
	} // End of calculate function.

	var g errgroup.Group
	for i := range numGo {
		start := i * width
		end := start + width
		if end > srcFn.n {
			end = srcFn.n
		}
		g.Go(func() error {
			return calculate(start, end)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// All goroutines are done. Now attach their results.
	out := args.out
	for _, chunk := range outputs {
		out.UidMatrix = append(out.UidMatrix, chunk.UidMatrix...)
		out.Counts = append(out.Counts, chunk.Counts...)
		out.ValueMatrix = append(out.ValueMatrix, chunk.ValueMatrix...)
		out.FacetMatrix = append(out.FacetMatrix, chunk.FacetMatrix...)
		out.LangMatrix = append(out.LangMatrix, chunk.LangMatrix...)
	}
	return nil
}

func facetsFilterValuePostingList(args funcArgs, pl *posting.List, facetsTree *facetsTree,
	listType bool, fn func(p *pb.Posting)) error {
	q := args.q

	var langMatch *pb.Posting
	var err error

	// We need to pick multiple postings only in two cases:
	// 1. ExpandAll is true.
	// 2. Attribute type is of list type and no lang tag is specified in query.
	pickMultiplePostings := q.ExpandAll || (listType && len(q.Langs) == 0)

	if !pickMultiplePostings {
		// Retrieve the posting that matches the language preferences.
		langMatch, err = pl.PostingFor(q.ReadTs, q.Langs)
		if err != nil && err != posting.ErrNoValue {
			return err
		}
	}

	// TODO(Ashish): This function starts iteration from start(afterUID is always 0). This can be
	// optimized in come cases. For example when we know lang tag to fetch, we can directly jump
	// to posting starting with that UID(check list.ValueFor()).
	return pl.Iterate(q.ReadTs, 0, func(p *pb.Posting) error {
		if q.ExpandAll {
			// If q.ExpandAll is true we need to consider all postings irrespective of langs.
		} else if listType && len(q.Langs) == 0 {
			// Don't retrieve tagged values unless explicitly asked.
			if len(p.LangTag) > 0 {
				return nil
			}
		} else {
			// Only consider the posting that matches our language preferences.
			if !proto.Equal(p, langMatch) {
				return nil
			}
		}

		// If filterTree is nil, applyFacetsTree returns true and nil error.
		picked, err := applyFacetsTree(p.Facets, facetsTree)
		if err != nil {
			return err
		}
		if picked {
			fn(p)
		}

		if pickMultiplePostings {
			return nil // Continue iteration.
		}

		// We have picked the right posting, we can stop iteration now.
		return posting.ErrStopIteration
	})
}

func countForValuePostings(args funcArgs, pl *posting.List, facetsTree *facetsTree,
	listType bool) (int, error) {
	var filteredCount int
	err := facetsFilterValuePostingList(args, pl, facetsTree, listType, func(p *pb.Posting) {
		filteredCount++
	})
	if err != nil {
		return 0, err
	}

	return filteredCount, nil
}

func retrieveValuesAndFacets(args funcArgs, pl *posting.List, facetsTree *facetsTree,
	listType bool) ([]types.Val, *pb.FacetsList, error) {
	q := args.q
	var vals []types.Val
	var fcs []*pb.Facets

	err := facetsFilterValuePostingList(args, pl, facetsTree, listType, func(p *pb.Posting) {
		vals = append(vals, types.Val{
			Tid:   types.TypeID(p.ValType),
			Value: p.Value,
		})
		if q.FacetParam != nil {
			fcs = append(fcs, &pb.Facets{Facets: facets.CopyFacets(p.Facets, q.FacetParam)})
		}
	})
	if err != nil {
		return nil, nil, err
	}

	return vals, &pb.FacetsList{FacetsList: fcs}, nil
}

func facetsFilterUidPostingList(pl *posting.List, facetsTree *facetsTree, opts posting.ListOptions,
	fn func(*pb.Posting)) error {

	return pl.Postings(opts, func(p *pb.Posting) error {
		// If filterTree is nil, applyFacetsTree returns true and nil error.
		pick, err := applyFacetsTree(p.Facets, facetsTree)
		if err != nil {
			return err
		}
		if pick {
			fn(p)
		}
		return nil
	})
}

func countForUidPostings(args funcArgs, pl *posting.List, facetsTree *facetsTree,
	opts posting.ListOptions) (int, error) {

	var filteredCount int
	err := facetsFilterUidPostingList(pl, facetsTree, opts, func(p *pb.Posting) {
		filteredCount++
	})
	if err != nil {
		return 0, err
	}

	return filteredCount, nil
}

func retrieveUidsAndFacets(args funcArgs, pl *posting.List, facetsTree *facetsTree,
	opts posting.ListOptions) (*pb.List, []*pb.Facets, error) {
	q := args.q

	var fcsList []*pb.Facets
	uidList := &pb.List{
		Uids: make([]uint64, 0, pl.ApproxLen()), // preallocate uid slice.
	}

	err := facetsFilterUidPostingList(pl, facetsTree, opts, func(p *pb.Posting) {
		uidList.Uids = append(uidList.Uids, p.Uid)
		if q.FacetParam != nil {
			fcsList = append(fcsList, &pb.Facets{
				Facets: facets.CopyFacets(p.Facets, q.FacetParam),
			})
		}
	})
	if err != nil {
		return nil, nil, err
	}

	return uidList, fcsList, nil
}

// This function handles operations on uid posting lists. Index keys, reverse keys and some data
// keys store uid posting lists.
// 此函数处理uid posting lists上的操作。索引键、反向键和一些数据键存储uid发布列表。
func (qs *queryState) handleUidPostings(
	ctx context.Context, args funcArgs, opts posting.ListOptions) error {
	srcFn := args.srcFn // 获取查询函数对象
	q := args.q  // 获取查询任务对象

	//得到facet树
	facetsTree, err := preprocessFilter(q.FacetsFilter) 
	if err != nil {
		return err
	}

	//下面这一块是记录一些状态与操作时间间隔
	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, "handleUidPostings") // SpanTimer返回一个函数，用于记录给定跨度的持续时间。
	defer stop()
	if span != nil {
		span.Annotatef(nil, "Number of uids: %d. args.srcFn: %+v", srcFn.n, args.srcFn) // 添加一个带有属性的注释
	}
	if srcFn.n == 0 {
		return nil
	}

	// srcFn.n should be equal to len(q.UidList.Uids) for below implementation(DivideAndRule and
	// calculate) to work correctly. But we have seen some panics while forming DataKey in
	// calculate(). panic is of the form "index out of range [4] with length 1". Hence return error
	// from here when srcFn.n != len(q.UidList.Uids).
	// srcFn.n应等于len（q.UidList.Uids），以便以下实现（DivideAndRule和compute）正常工作。但是我们在calculate（）中形成DataKey时看到了一些恐慌。恐慌的形式是“索引超出范围[4]，长度为1”。因此，当srcFn.n！=len（q.UidList.Uids）。

	switch srcFn.fnType {
	case notAFunction, compareScalarFn, hasFn, uidInFn:
		if srcFn.n != len(q.UidList.GetUids()) { // 如果函数类别是0,3,8,9的话，且查询函数内的目标数量与当前层待查的UID数量不相同，那就是有错误
			return errors.Errorf("srcFn.n: %d is not equal to len(q.UidList.Uids): %d, srcFn: %+v in "+
				"handleUidPostings", srcFn.n, len(q.UidList.GetUids()), srcFn)
		}
	}

	// Divide the task into many goroutines.
	// 将任务划分为多个协程来处理
	numGo, width := x.DivideAndRule(srcFn.n) // width是单个协程的处理宽度（诺为uid，那么就是uid处理数量）
	x.AssertTrue(width > 0)
	span.Annotatef(nil, "Width: %d. NumGo: %d", width, numGo)

	// 有关多语言处理
	lang := langForFunc(q.Langs)
	needFiltering := needsStringFiltering(srcFn, q.Langs, q.Attr)
	isList := schema.State().IsList(q.Attr)

	//根据上面的得到的协程数量，创建阻塞切片errCh与汇总输出切片outputs
	errCh := make(chan error, numGo)
	outputs := make([]*pb.Result, numGo)


	// 下面这个函数是最最核心的处理
	calculate := func(start, end int) error {
		x.AssertTrue(start%width == 0)
		out := &pb.Result{} // 创建结果输出结构体
		outputs[start/width] = out // 赋给结果汇总结构体

		// 遍历依次处理每一个目标
		for i := start; i < end; i++ {
			if i%100 == 0 { // 非阻塞地检查上下文状态，在执行操作前先确认是否应该继续执行
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			var key []byte
			switch srcFn.fnType {
			case notAFunction, compareScalarFn, hasFn, uidInFn: // 如果是有当前层待查寻UID列表的查询，生成具有给定属性和UID的数据key，其内主要包含谓词attr与目标UID
				if q.Reverse { // 是否是反转查询
					key = x.ReverseKey(q.Attr, q.UidList.Uids[i])
				} else {
					key = x.DataKey(q.Attr, q.UidList.Uids[i])
				}
			case geoFn, regexFn, fullTextSearchFn, standardFn, customIndexFn, matchFn, // 如果是其他的查询，生成具有给定属性和术语的索引键
				compareAttrFn:
				key = x.IndexKey(q.Attr, srcFn.tokens[i])
			default:
				return errors.Errorf("Unhandled function in handleUidPostings: %s", srcFn.fname)
			}

			// Get or create the posting list for an entity, attribute combination.
			pl, err := qs.cache.Get(key) // NOTE:核心操作，根据key生成一个posting-list对象，其内已经有目标UID了（在(*(*(*(*pl).plist).Pack).Blocks[0]).Base中）
			if err != nil {
				return err
			}

			if srcFn.fnType == compareAttrFn {
				posting.GetStatsHolder().InsertRecord(
					q.Attr, []byte(srcFn.tokens[i]), uint64(pl.ApproxLen()))
			}

			// zzlTODO:UID查询的另一部分，下面这个switch是对结果筛选吗？
			switch {
			case q.DoCount:
				if i == 0 {
					span.Annotate(nil, "DoCount")
				}
				count, err := countForUidPostings(args, pl, facetsTree, opts)
				if err != nil {
					return err
				}
				out.Counts = append(out.Counts, uint32(count))
				// Add an empty UID list to make later processing consistent.
				out.UidMatrix = append(out.UidMatrix, &pb.List{})
			case srcFn.fnType == compareScalarFn:
				if i == 0 {
					span.Annotate(nil, "CompareScalarFn")
				}
				len := pl.Length(args.q.ReadTs, 0)
				if len == -1 {
					return errors.Wrapf(posting.ErrTsTooOld, "While reading posting list length")
				}
				count := int64(len)
				if evalCompare(srcFn.fname, count, srcFn.threshold[0]) {
					tlist := &pb.List{Uids: []uint64{q.UidList.Uids[i]}}
					out.UidMatrix = append(out.UidMatrix, tlist)
				}
			case srcFn.fnType == hasFn:
				if i == 0 {
					span.Annotate(nil, "HasFn")
				}
				// We figure out if need to filter on bases of lang attribute or not.
				// If we don't need to do so, we can just check if the posting list
				// is empty. If we need to filter on basis of lang, we need to check
				// the value with its tag. if lang == "", in that case, we need to
				// return if there are any untagged values. If lang != "", in that
				// case we need to check exact value.
				empty := false
				var err error
				if !needFiltering {
					empty, err = pl.IsEmpty(args.q.ReadTs, 0)
				} else {
					if lang == "" {
						if isList {
							_, err = pl.AllValues(args.q.ReadTs)
						} else {
							_, err = pl.Value(args.q.ReadTs)
						}
					} else {
						_, err = pl.ValueForTag(args.q.ReadTs, lang)
					}

					if err == posting.ErrNoValue {
						empty = true
						err = nil
					}
				}
				if err != nil {
					return err
				}
				if !empty {
					tlist := &pb.List{Uids: []uint64{q.UidList.Uids[i]}}
					out.UidMatrix = append(out.UidMatrix, tlist)
				}
			case srcFn.fnType == uidInFn:
				if i == 0 {
					span.Annotate(nil, "UidInFn")
				}
				reqList := &pb.List{Uids: srcFn.uidsPresent}
				topts := posting.ListOptions{
					ReadTs:    args.q.ReadTs,
					AfterUid:  0,
					Intersect: reqList,
					First:     int(args.q.First + args.q.Offset),
				}
				plist, err := pl.Uids(topts)
				if err != nil {
					return err
				}
				if len(plist.Uids) > 0 {
					tlist := &pb.List{Uids: []uint64{q.UidList.Uids[i]}}
					out.UidMatrix = append(out.UidMatrix, tlist)
				}
			case q.FacetParam != nil || facetsTree != nil:
				if i == 0 {
					span.Annotate(nil, "default with facets")
				}
				uidList, fcsList, err := retrieveUidsAndFacets(args, pl, facetsTree, opts)
				if err != nil {
					return err
				}
				out.UidMatrix = append(out.UidMatrix, uidList)
				if q.FacetParam != nil {
					out.FacetMatrix = append(out.FacetMatrix, &pb.FacetsList{FacetsList: fcsList})
				}
			default:
				if i == 0 {
					span.Annotate(nil, "default no facets")
				}
				uidList, err := pl.Uids(opts) // NOTE:核心操作，应该是从pl中筛选出来目标UID，opts是列表配置项，内有读时间戳，其内的大部分值由查询任务赋予
				if err != nil {
					return err
				}
				out.UidMatrix = append(out.UidMatrix, uidList)
			}
		}
		return nil
	} // End of calculate function.

	for i := range numGo { // 这个for循环是为了开启go协程开始计算任务
		start := i * width
		end := start + width
		if end > srcFn.n {
			end = srcFn.n
		}
		go func(start, end int) {
			errCh <- calculate(start, end) // 分配并查询计算
		}(start, end)
	}
	for range numGo {  // 这个for循环等待所有go协程完成任务
		if err := <-errCh; err != nil {
			return err
		}
	}
	// All goroutines are done. Now attach their results.
	// 所有的协程都结束了。现在集成他们的结果,注意下面这个out是直接取得args的地址，所以会同步改。
	out := args.out
	for _, chunk := range outputs {
		out.FacetMatrix = append(out.FacetMatrix, chunk.FacetMatrix...)
		out.Counts = append(out.Counts, chunk.Counts...)
		out.UidMatrix = append(out.UidMatrix, chunk.UidMatrix...) //集成查到的UID
	}
	var total int
	for _, list := range out.UidMatrix {
		total += len(list.Uids)
	}
	span.Annotatef(nil, "Total number of elements in matrix: %d", total)
	return nil
}

const (
	// UseTxnCache indicates the transaction cache should be used. UseTxnCache表示应使用事务缓存。
	UseTxnCache = iota
	// NoCache indicates no caches should be used. NoCache表示不应使用缓存。
	NoCache
)

// processTask processes the query, accumulates and returns the result.
// processTask处理当前谓词查询，累加并返回结果。
func processTask(ctx context.Context, q *pb.Query, gid uint32) (*pb.Result, error) {
	ctx, span := otrace.StartSpan(ctx, "processTask."+q.Attr)
	defer span.End()

	stop := x.SpanTimer(span, "processTask"+q.Attr)
	defer stop()

	span.Annotatef(nil, "Waiting for startTs: %d at node: %d, gid: %d",
		q.ReadTs, groups().Node.Id, gid)
	if err := posting.Oracle().WaitForTs(ctx, q.ReadTs); err != nil {
		return nil, err
	}
	if span != nil {
		maxAssigned := posting.Oracle().MaxAssigned()
		span.Annotatef(nil, "Done waiting for maxAssigned. Attr: %q ReadTs: %d Max: %d",
			q.Attr, q.ReadTs, maxAssigned)
	}
	if err := groups().ChecksumsMatch(ctx); err != nil {
		return nil, err
	}
	span.Annotatef(nil, "Done waiting for checksum match")

	//下面这一块应该只是增强某种健壮性的代码，避免特殊情况的出现
	// If a group stops serving tablet and it gets partitioned away from group
	// zero, then it wouldn't know that this group is no longer serving this
	// predicate. There's no issue if a we are serving a particular tablet and
	// we get partitioned away from group zero as long as it's not removed.
	// BelongsToReadOnly is called instead of BelongsTo to prevent this alpha
	// from requesting to serve this tablet.
	// 如果一个组停止为某个tablet提供服务，并且它被从组0中分区出来，那么它就不会知道这个组不再为这个谓词提供服务。
	// 如果我们正在为特定的tablet提供服务，并且只要它没有被删除，我们就可以从零组分区。
	// 调用BelongsToReadOnly而不是BelongsTo，以防止此alpha请求提供此tablet。
	knownGid, err := groups().BelongsToReadOnly(q.Attr, q.ReadTs)
	switch {
	case err != nil:
		return nil, err
	case knownGid == 0:
		return nil, errNonExistentTablet
	case knownGid != groups().groupId():
		return nil, errUnservedTablet
	}

	var qs queryState // 查询的状态的暂时保存，即缓存，其本质是一个LocalCache对象 NOTE:202506051
	if q.Cache == UseTxnCache {
		qs.cache = posting.Oracle().CacheAt(q.ReadTs) // oracle是一个全局的管理事物的对象，分配一个缓存
	}
	if qs.cache == nil {
		qs.cache = posting.NoCache(q.ReadTs) // 创建一个本地缓存，但是这个缓存只用来存储当前查询任务的ReadTs（startTs）
	}
	// For now, remove the query level cache. It is causing contention for queries with high fan-out.
	// 到现在为止，删除查询级缓存。这导致了对高扇出查询的争用。
	out, err := qs.helpProcessTask(ctx, q, gid) // NOTE:核心操作，真正的查询在此
	if err != nil {
		return nil, err
	}
	return out, nil
}

type queryState struct {
	cache *posting.LocalCache
}

func (qs *queryState) helpProcessTask(ctx context.Context, q *pb.Query, gid uint32) (
	*pb.Result, error) {

	// FromContext返回存储在上下文中的Span，或者如果没有记录事件，则返回不记录事件的Span。
	span := otrace.FromContext(ctx)
	out := new(pb.Result)
	attr := q.Attr

	srcFn, err := parseSrcFn(ctx, q) // NOTE:核心操作，解析当前查询任务给出查询函数对象并且根据函数类别填充它
	if err != nil {
		return nil, err
	}

	//下面是结合查询任务与查询函数并且根据schema中现有对谓词的定义来判断当前查询是否合法
	if q.Reverse && !schema.State().IsReversed(ctx, attr) { // 有关反转边
		return nil, errors.Errorf("Predicate %s doesn't have reverse edge", x.ParseAttr(attr))
	}

	if needsIndex(srcFn.fnType, q.UidList) && !schema.State().IsIndexed(ctx, q.Attr) { // 有关索引
		return nil, errors.Errorf("Predicate %s is not indexed", x.ParseAttr(q.Attr))
	}

	if len(q.Langs) > 0 && !schema.State().HasLang(attr) { // 有关多语言的
		return nil, errors.Errorf("Language tags can only be used with predicates of string type"+
			" having @lang directive in schema. Got: [%v]", x.ParseAttr(attr))
	}
	if len(q.Langs) == 1 && q.Langs[0] == "*" { // 同有关多语言的
		// Reset the Langs fields. The ExpandAll field is set to true already so there's no
		// more need to store the star value in this field.
		q.Langs = nil
	}

	typ, err := schema.State().TypeOf(attr) // 得到谓词的属性类别type
	if err != nil {
		// All schema checks are done before this, this type is only used to
		// convert it to schema type before returning.
		// Schema type won't be present only if there is no data for that predicate
		// or if we load through bulk loader.
		// 所有模式检查都在此之前完成，此类型仅用于在返回之前将其转换为模式类型。
		// 只有当该谓词没有数据或我们通过批量加载器加载时，Schema类型才会出现。
		typ = types.DefaultID
	}
	out.List = schema.State().IsList(attr) //IsList返回谓词是否为列表类型。
	srcFn.atype = typ // 设置查询函数的谓词对象类别

	// Reverse attributes might have more than 1 results even if the original attribute is not a list.
	// 即使原始属性不是列表，反向属性也可能有多个结果。
	if q.Reverse {
		out.List = true
	}

	// ListOptions在List中使用。Uids（在发布中）为每个发布列表定制我们的UID输出列表。这个包裹应该是pb。
	opts := posting.ListOptions{
		ReadTs:   q.ReadTs,
		AfterUid: q.AfterUid,
		First:    int(q.First + q.Offset),
	}
	// If we have srcFunc and Uids, it means its a filter. So we intersect.
	// 如果我们有srcFunc和Uids，这意味着它是一个过滤器。所以我们相交。
	if srcFn.fnType != notAFunction && q.UidList != nil && len(q.UidList.Uids) > 0 {
		// 过滤器filter才会进入这里
		// var xxx=q.UidList
		// fmt.Print(xxx)
		opts.Intersect = q.UidList
	}

	args := funcArgs{q, gid, srcFn, out}
	// needsValuePostings根据谓词的类别来判断获取值的posting lists还是uid的posting lists。（即如果是获取节点的，就为是要获取uid的，此时便为false，否则就是获取谓词对应的宾语的值的，为true）
	needsValPostings, err := srcFn.needsValuePostings(typ) 
	if err != nil {
		return nil, err
	}
	// NOTE:核心操作，下面俩是最核心的操作，调用对应的函数获取数据（out在args内的，）
	if needsValPostings { // 如果是要获取值的posting list
		span.Annotate(nil, "handleValuePostings")
		if err := qs.handleValuePostings(ctx, args); err != nil { //NOTE:核心操作，查询目标谓词所对应的值 // zzlTODO:待看，怎么读值
			return nil, err
		}
	} else {
		span.Annotate(nil, "handleUidPostings")
		if err = qs.handleUidPostings(ctx, args, opts); err != nil { //NOTE:核心操作，查询目标节点的UID 
			return nil, err
		}
	}

	if srcFn.fnType == hasFn && srcFn.isFuncAtRoot {
		span.Annotate(nil, "handleHasFunction")
		if err := qs.handleHasFunction(ctx, q, out, srcFn); err != nil {
			return nil, err
		}
	}

	if srcFn.fnType == compareScalarFn && srcFn.isFuncAtRoot {
		span.Annotate(nil, "handleCompareScalarFunction")
		if err := qs.handleCompareScalarFunction(ctx, args); err != nil {
			return nil, err
		}
	}

	if srcFn.fnType == regexFn {
		span.Annotate(nil, "handleRegexFunction")
		if err := qs.handleRegexFunction(ctx, args); err != nil {
			return nil, err
		}
	}

	if srcFn.fnType == matchFn {
		span.Annotate(nil, "handleMatchFunction")
		if err := qs.handleMatchFunction(ctx, args); err != nil {
			return nil, err
		}
	}

	// We fetch the actual value for the uids, compare them to the value in the
	// request and filter the uids only if the tokenizer IsLossy.
	if srcFn.fnType == compareAttrFn && len(srcFn.tokens) > 0 {
		span.Annotate(nil, "handleCompareFunction")
		if err := qs.handleCompareFunction(ctx, args); err != nil {
			return nil, err
		}
	}

	// If geo filter, do value check for correctness.
	if srcFn.geoQuery != nil {
		span.Annotate(nil, "handleGeoFunction")
		if err := qs.filterGeoFunction(ctx, args); err != nil {
			return nil, err
		}
	}

	// For string matching functions, check the language. We are not checking here
	// for hasFn as filtering for it has already been done in handleHasFunction.
	if srcFn.fnType != hasFn && needsStringFiltering(srcFn, q.Langs, attr) {
		span.Annotate(nil, "filterStringFunction")
		if err := qs.filterStringFunction(args); err != nil {
			return nil, err
		}
	}

	out.IntersectDest = srcFn.intersectDest
	return out, nil
}

func needsStringFiltering(srcFn *functionContext, langs []string, attr string) bool {
	if !srcFn.isStringFn {
		return false
	}

	// If a predicate doesn't have @lang directive in schema, we don't need to do any string
	// filtering.
	if !schema.State().HasLang(attr) {
		return false
	}

	return langForFunc(langs) != "." &&
		(srcFn.fnType == standardFn || srcFn.fnType == hasFn ||
			srcFn.fnType == fullTextSearchFn || srcFn.fnType == compareAttrFn ||
			srcFn.fnType == customIndexFn)
}

func (qs *queryState) handleCompareScalarFunction(ctx context.Context, arg funcArgs) error {
	attr := arg.q.Attr
	if ok := schema.State().HasCount(ctx, attr); !ok {
		return errors.Errorf("Need @count directive in schema for attr: %s for fn: %s at root",
			x.ParseAttr(attr), arg.srcFn.fname)
	}
	counts := arg.srcFn.threshold
	cp := countParams{
		fn:      arg.srcFn.fname,
		counts:  counts,
		attr:    attr,
		gid:     arg.gid,
		readTs:  arg.q.ReadTs,
		reverse: arg.q.Reverse,
	}
	return qs.evaluate(cp, arg.out)
}

func (qs *queryState) handleRegexFunction(ctx context.Context, arg funcArgs) error {
	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, "handleRegexFunction")
	defer stop()
	if span != nil {
		span.Annotatef(nil, "Number of uids: %d. args.srcFn: %+v", arg.srcFn.n, arg.srcFn)
	}

	attr := arg.q.Attr
	typ, err := schema.State().TypeOf(attr)
	span.Annotatef(nil, "Attr: %s. Type: %s", attr, typ.Name())
	if err != nil || !typ.IsScalar() {
		return errors.Errorf("Attribute not scalar: %s %v", x.ParseAttr(attr), typ)
	}
	if typ != types.StringID {
		return errors.Errorf("Got non-string type. Regex match is allowed only on string type.")
	}
	useIndex := schema.State().HasTokenizer(ctx, tok.IdentTrigram, attr)
	span.Annotatef(nil, "Trigram index found: %t, func at root: %t",
		useIndex, arg.srcFn.isFuncAtRoot)

	query := cindex.RegexpQuery(arg.srcFn.regex.Syntax)
	empty := pb.List{}
	var uids *pb.List

	// Here we determine the list of uids to match.
	switch {
	// If this is a filter eval, use the given uid list (good)
	case arg.q.UidList != nil:
		// These UIDs are copied into arg.out.UidMatrix which is later updated while
		// processing the query. The below trick makes a copy of the list to avoid the
		// race conditions later. I (Aman) did a race condition tests to ensure that we
		// do not have more race condition in similar code in the rest of the file.
		// The race condition was found only here because in filter condition, even when
		// predicates do not have indexes, we allow regexp queries (for example, we do
		// not support eq/gt/lt/le in @filter, see #4077), and this was new code that
		// was added just to support the aforementioned case, the race condition is only
		// in this part of the code.
		uids = &pb.List{}
		uids.Uids = append(arg.q.UidList.Uids[:0:0], arg.q.UidList.Uids...)

	// Prefer to use an index (fast)
	case useIndex:
		uids, err = uidsForRegex(attr, arg, query, &empty)
		if err != nil {
			return err
		}

	// No index and at root, return error instructing user to use `has` or index.
	default:
		return errors.Errorf(
			"Attribute %v does not have trigram index for regex matching. "+
				"Please add a trigram index or use has/uid function with regexp() as filter.",
			x.ParseAttr(attr))
	}

	arg.out.UidMatrix = append(arg.out.UidMatrix, uids)
	isList := schema.State().IsList(attr)
	lang := langForFunc(arg.q.Langs)

	span.Annotatef(nil, "Total uids: %d, list: %t lang: %v", len(uids.Uids), isList, lang)

	filtered := &pb.List{}
	for _, uid := range uids.Uids {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		pl, err := qs.cache.Get(x.DataKey(attr, uid))
		if err != nil {
			return err
		}

		vals := make([]types.Val, 1)
		switch {
		case lang != "":
			vals[0], err = pl.ValueForTag(arg.q.ReadTs, lang)

		case isList:
			vals, err = pl.AllUntaggedValues(arg.q.ReadTs)

		default:
			vals[0], err = pl.Value(arg.q.ReadTs)
		}
		if err != nil {
			if err == posting.ErrNoValue {
				continue
			}
			return err
		}

		for _, val := range vals {
			// convert data from binary to appropriate format
			strVal, err := types.Convert(val, types.StringID)
			if err == nil && matchRegex(strVal, arg.srcFn.regex) {
				filtered.Uids = append(filtered.Uids, uid)
				// NOTE: We only add the uid once.
				break
			}
		}
	}

	for i := range arg.out.UidMatrix {
		algo.IntersectWith(arg.out.UidMatrix[i], filtered, arg.out.UidMatrix[i])
	}

	return nil
}

func (qs *queryState) handleCompareFunction(ctx context.Context, arg funcArgs) error {
	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, "handleCompareFunction")
	defer stop()
	if span != nil {
		span.Annotatef(nil, "Number of uids: %d. args.srcFn: %+v", arg.srcFn.n, arg.srcFn)
	}

	attr := arg.q.Attr
	span.Annotatef(nil, "Attr: %s. Fname: %s", attr, arg.srcFn.fname)
	tokenizer, err := pickTokenizer(ctx, attr, arg.srcFn.fname)
	if err != nil {
		return err
	}

	// Only if the tokenizer that we used IsLossy
	// then we need to fetch and compare the actual values.
	span.Annotatef(nil, "Tokenizer: %s, Lossy: %t", tokenizer.Name(), tokenizer.IsLossy())

	if !tokenizer.IsLossy() {
		return nil
	}

	// Need to evaluate inequality for entries in the first bucket.
	typ, err := schema.State().TypeOf(attr)
	if err != nil || !typ.IsScalar() {
		return errors.Errorf("Attribute not scalar: %s %v", x.ParseAttr(attr), typ)
	}

	x.AssertTrue(len(arg.out.UidMatrix) > 0)
	isList := schema.State().IsList(attr)
	lang := langForFunc(arg.q.Langs)

	filterRow := func(row int, compareFunc func(types.Val) bool) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var filterErr error
		algo.ApplyFilter(arg.out.UidMatrix[row], func(uid uint64, i int) bool {
			switch lang {
			case "":
				if isList {
					pl, err := posting.GetNoStore(x.DataKey(attr, uid), arg.q.ReadTs)
					if err != nil {
						filterErr = err
						return false
					}
					svs, err := pl.AllUntaggedValues(arg.q.ReadTs)
					if err != nil {
						if err != posting.ErrNoValue {
							filterErr = err
						}
						return false
					}
					for _, sv := range svs {
						dst, err := types.Convert(sv, typ)
						if err == nil && compareFunc(dst) {
							return true
						}
					}

					return false
				}

				pl, err := posting.GetNoStore(x.DataKey(attr, uid), arg.q.ReadTs)
				if err != nil {
					filterErr = err
					return false
				}
				sv, err := pl.Value(arg.q.ReadTs)
				if err != nil {
					if err != posting.ErrNoValue {
						filterErr = err
					}
					return false
				}
				dst, err := types.Convert(sv, typ)
				return err == nil && compareFunc(dst)
			case ".":
				pl, err := posting.GetNoStore(x.DataKey(attr, uid), arg.q.ReadTs)
				if err != nil {
					filterErr = err
					return false
				}
				values, err := pl.AllValues(arg.q.ReadTs) // does not return ErrNoValue
				if err != nil {
					filterErr = err
					return false
				}
				for _, sv := range values {
					dst, err := types.Convert(sv, typ)
					if err == nil && compareFunc(dst) {
						return true
					}
				}
				return false
			default:
				sv, err := fetchValue(uid, attr, arg.q.Langs, typ, arg.q.ReadTs)
				if err != nil {
					if err != posting.ErrNoValue {
						filterErr = err
					}
					return false
				}
				if sv.Value == nil {
					return false
				}
				return compareFunc(sv)
			}
		})
		if filterErr != nil {
			return err
		}

		return nil
	}

	switch {
	case arg.srcFn.fname == eq:
		// If fn is eq, we could have multiple arguments and hence multiple rows to filter.
		for row := range arg.srcFn.tokens {
			compareFunc := func(dst types.Val) bool {
				return types.CompareVals(arg.srcFn.fname, dst, arg.srcFn.eqTokens[row])
			}
			if err := filterRow(row, compareFunc); err != nil {
				return err
			}
		}
	case arg.srcFn.fname == between:
		compareFunc := func(dst types.Val) bool {
			return types.CompareBetween(dst, arg.srcFn.eqTokens[0], arg.srcFn.eqTokens[1])
		}
		if err := filterRow(0, compareFunc); err != nil {
			return err
		}
		if err := filterRow(len(arg.out.UidMatrix)-1, compareFunc); err != nil {
			return err
		}
	case arg.srcFn.tokens[0] == arg.srcFn.ineqValueToken[0]:
		// If operation is not eq and ineqValueToken equals first token,
		// then we need to filter first row.
		compareFunc := func(dst types.Val) bool {
			return types.CompareVals(arg.q.SrcFunc.Name, dst, arg.srcFn.eqTokens[0])
		}
		if err := filterRow(0, compareFunc); err != nil {
			return err
		}
	}

	return nil
}

func (qs *queryState) handleMatchFunction(ctx context.Context, arg funcArgs) error {
	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, "handleMatchFunction")
	defer stop()
	if span != nil {
		span.Annotatef(nil, "Number of uids: %d. args.srcFn: %+v", arg.srcFn.n, arg.srcFn)
	}

	attr := arg.q.Attr
	typ := arg.srcFn.atype
	span.Annotatef(nil, "Attr: %s. Type: %s", attr, typ.Name())
	var uids *pb.List
	switch {
	case !typ.IsScalar():
		return errors.Errorf("Attribute not scalar: %s %v", attr, typ)

	case typ != types.StringID:
		return errors.Errorf("Got non-string type. Fuzzy match is allowed only on string type.")

	case arg.q.UidList != nil && len(arg.q.UidList.Uids) != 0:
		uids = arg.q.UidList

	case schema.State().HasTokenizer(ctx, tok.IdentTrigram, attr):
		var err error
		uids, err = uidsForMatch(attr, arg)
		if err != nil {
			return err
		}

	default:
		return errors.Errorf(
			"Attribute %v does not have trigram index for fuzzy matching. "+
				"Please add a trigram index or use has/uid function with match() as filter.",
			x.ParseAttr(attr))
	}

	isList := schema.State().IsList(attr)
	lang := langForFunc(arg.q.Langs)
	span.Annotatef(nil, "Total uids: %d, list: %t lang: %v", len(uids.Uids), isList, lang)
	arg.out.UidMatrix = append(arg.out.UidMatrix, uids)

	matchQuery := strings.Join(arg.srcFn.tokens, "")
	filtered := &pb.List{}
	for _, uid := range uids.Uids {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		pl, err := qs.cache.Get(x.DataKey(attr, uid))
		if err != nil {
			return err
		}

		vals := make([]types.Val, 1)
		switch {
		case lang != "":
			vals[0], err = pl.ValueForTag(arg.q.ReadTs, lang)

		case isList:
			vals, err = pl.AllUntaggedValues(arg.q.ReadTs)

		default:
			vals[0], err = pl.Value(arg.q.ReadTs)
		}
		if err != nil {
			if err == posting.ErrNoValue {
				continue
			}
			return err
		}

		max := int(arg.srcFn.threshold[0])
		for _, val := range vals {
			// convert data from binary to appropriate format
			strVal, err := types.Convert(val, types.StringID)
			if err == nil && matchFuzzy(matchQuery, strVal.Value.(string), max) {
				filtered.Uids = append(filtered.Uids, uid)
				// NOTE: We only add the uid once.
				break
			}
		}
	}

	for i := range arg.out.UidMatrix {
		algo.IntersectWith(arg.out.UidMatrix[i], filtered, arg.out.UidMatrix[i])
	}

	return nil
}

func (qs *queryState) filterGeoFunction(ctx context.Context, arg funcArgs) error {
	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, "filterGeoFunction")
	defer stop()

	attr := arg.q.Attr
	uids := algo.MergeSorted(arg.out.UidMatrix)
	numGo, width := x.DivideAndRule(len(uids.Uids))
	if span != nil && numGo > 1 {
		span.Annotatef(nil, "Number of uids: %d. NumGo: %d. Width: %d\n",
			len(uids.Uids), numGo, width)
	}

	filtered := make([]*pb.List, numGo)
	filter := func(idx, start, end int) error {
		filtered[idx] = &pb.List{}
		out := filtered[idx]
		for _, uid := range uids.Uids[start:end] {
			pl, err := qs.cache.Get(x.DataKey(attr, uid))
			if err != nil {
				return err
			}
			var tv pb.TaskValue
			err = pl.Iterate(arg.q.ReadTs, 0, func(p *pb.Posting) error {
				tv.ValType = p.ValType
				tv.Val = p.Value
				if types.MatchGeo(&tv, arg.srcFn.geoQuery) {
					out.Uids = append(out.Uids, uid)
					return posting.ErrStopIteration
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	}

	errCh := make(chan error, numGo)
	for i := range numGo {
		start := i * width
		end := start + width
		if end > len(uids.Uids) {
			end = len(uids.Uids)
		}
		go func(idx, start, end int) {
			errCh <- filter(idx, start, end)
		}(i, start, end)
	}
	for range numGo {
		if err := <-errCh; err != nil {
			return err
		}
	}
	final := &pb.List{}
	for _, out := range filtered {
		final.Uids = append(final.Uids, out.Uids...)
	}
	if span != nil && numGo > 1 {
		span.Annotatef(nil, "Total uids after filtering geo: %d", len(final.Uids))
	}
	for i := range arg.out.UidMatrix {
		algo.IntersectWith(arg.out.UidMatrix[i], final, arg.out.UidMatrix[i])
	}
	return nil
}

// TODO: This function is really slow when there are a lot of UIDs to filter, for e.g. when used in
// `has(name)`. We could potentially have a query level cache, which can be used to speed things up
// a bit. Or, try to reduce the number of UIDs which make it here.
func (qs *queryState) filterStringFunction(arg funcArgs) error {
	if glog.V(3) {
		glog.Infof("filterStringFunction. arg: %+v\n", arg.q)
		defer glog.Infof("Done filterStringFunction")
	}
	attr := arg.q.Attr
	uids := algo.MergeSorted(arg.out.UidMatrix)
	var values [][]types.Val
	filteredUids := make([]uint64, 0, len(uids.Uids))
	lang := langForFunc(arg.q.Langs)

	// This iteration must be done in a serial order, because we're also storing the values in a
	// matrix, to check it later.
	// TODO: This function can be optimized by having a query specific cache, which can be populated
	// by the handleHasFunction for e.g. for a `has(name)` query.
	for _, uid := range uids.Uids {
		vals, err := qs.getValsForUID(attr, lang, uid, arg.q.ReadTs)
		switch {
		case err == posting.ErrNoValue:
			continue
		case err != nil:
			return err
		}

		var strVals []types.Val
		for _, v := range vals {
			// convert data from binary to appropriate format
			strVal, err := types.Convert(v, types.StringID)
			if err != nil {
				continue
			}
			strVals = append(strVals, strVal)
		}
		if len(strVals) > 0 {
			values = append(values, strVals)
			filteredUids = append(filteredUids, uid)
		}
	}

	filtered := &pb.List{Uids: filteredUids}
	filter := stringFilter{
		funcName: arg.srcFn.fname,
		funcType: arg.srcFn.fnType,
		lang:     lang,
	}

	switch arg.srcFn.fnType {
	case hasFn:
		// Dont do anything, as filtering based on lang is already
		// done above.
	case fullTextSearchFn:
		filter.tokens = arg.srcFn.tokens
		filter.match = defaultMatch
		filter.tokName = "fulltext"
		filtered = matchStrings(filtered, values, &filter)
	case standardFn:
		filter.tokens = arg.srcFn.tokens
		filter.match = defaultMatch
		filter.tokName = "term"
		filtered = matchStrings(filtered, values, &filter)
	case customIndexFn:
		filter.tokens = arg.srcFn.tokens
		filter.match = defaultMatch
		filter.tokName = arg.q.SrcFunc.Args[0]
		filtered = matchStrings(filtered, values, &filter)
	case compareAttrFn:
		// filter.ineqValue = arg.srcFn.ineqValue
		filter.eqVals = arg.srcFn.eqTokens
		filter.match = ineqMatch
		filtered = matchStrings(filtered, values, &filter)
	}

	for i := range arg.out.UidMatrix {
		algo.IntersectWith(arg.out.UidMatrix[i], filtered, arg.out.UidMatrix[i])
	}
	return nil
}

func (qs *queryState) getValsForUID(attr, lang string, uid, ReadTs uint64) ([]types.Val, error) {
	key := x.DataKey(attr, uid)
	pl, err := qs.cache.Get(key)
	if err != nil {
		return nil, err
	}

	var vals []types.Val
	var val types.Val
	if lang == "" {
		if schema.State().IsList(attr) {
			// NOTE: we will never reach here if this function is called from handleHasFunction, as
			// @lang is not allowed for list predicates.
			vals, err = pl.AllValues(ReadTs)
		} else {
			val, err = pl.Value(ReadTs)
			vals = append(vals, val)
		}
	} else {
		val, err = pl.ValueForTag(ReadTs, lang)
		vals = append(vals, val)
	}

	return vals, err
}

func matchRegex(value types.Val, regex *cregexp.Regexp) bool {
	return len(value.Value.(string)) > 0 && regex.MatchString(value.Value.(string), true, true) > 0
}

type functionContext struct {
	tokens        []string // tokens是一个列表，其内只包含当前函数涉及的一些变量值（包括字面量与变量）
	geoQuery      *types.GeoQueryData
	intersectDest bool // 检查函数类型是否需要algo，如果函数结果需要相交，则返回true，否则返回false。
	// eqTokens is used by compareAttr functions. It stores values corresponding to each
	// function argument. There could be multiple arguments to `eq` function but only one for
	// other compareAttr functions.
	// TODO(@Animesh): change field names which could explain their uses better. Check if we
	// really need all of ineqValue, eqTokens, tokens
	eqTokens       []types.Val
	ineqValueToken []string
	n              int // 该值有关查询的数量级问题，可以为len(fc.tokens)（即函数体参数个数），或者len(q.UidList.Uids)（即上层查询传递下来的有多少个uid）
	threshold      []int64
	uidsPresent    []uint64
	fname          string
	fnType         FuncType //当前查询的函数类别，如allofterms为100
	regex          *cregexp.Regexp
	isFuncAtRoot   bool
	isStringFn     bool
	atype          types.TypeID   //当前查询的谓词属性类别
	vectorInfo     []float32
	vectorUid      uint64
}

const (
	eq      = "eq" // equal
	between = "between"
)

func ensureArgsCount(srcFunc *pb.SrcFunction, expected int) error {
	if len(srcFunc.Args) != expected {
		return errors.Errorf("Function '%s' requires %d arguments, but got %d (%v)",
			srcFunc.Name, expected, len(srcFunc.Args), srcFunc.Args)
	}
	return nil
}

func checkRoot(q *pb.Query, fc *functionContext) {
	if q.UidList == nil {
		// Fetch Uids from Store and populate in q.UidList.
		fc.n = 0
		fc.isFuncAtRoot = true
	} else {
		fc.n = len(q.UidList.Uids)
	}
}

// We allow atmost one lang in functions. We can inline in 1.9.
func langForFunc(langs []string) string {
	x.AssertTrue(len(langs) <= 1)
	if len(langs) == 0 {
		return ""
	}
	return langs[0]
}

func planForEqFilter(fc *functionContext, pred string, uidlist []uint64) {
	checkUidEmpty := func(uids []uint64) bool {
		for _, i := range uids {
			if i == 0 {
				return false
			}
		}
		return true
	}

	if !checkUidEmpty(uidlist) {
		// We have a uid which has 0 in it. Mostly it would happen when there is only 0. But any one item
		// being 0 could cause the query planner to fail. In case of 0 being present, we neeed to query the
		// index itself.
		fc.n = len(fc.tokens)
		return
	}

	if uint64(len(uidlist)) < Config.TypeFilterUidLimit {
		fc.tokens = fc.tokens[:0]
		fc.n = len(uidlist)
		return
	}

	estimatedCount := uint64(0)
	gotEstimate := false
	for _, eqToken := range fc.tokens {
		count := posting.GetStatsHolder().ProcessEqPredicate(pred, []byte(eqToken))
		if count != math.MaxUint64 {
			estimatedCount += count
			gotEstimate = true
		} else {
			break
		}
	}

	if gotEstimate && estimatedCount == 0 {
		gotEstimate = false
	}

	// TODO make a different config
	if gotEstimate && uint64(len(uidlist)) < estimatedCount/Config.TypeFilterUidLimit {
		fc.tokens = fc.tokens[:0]
		fc.n = len(uidlist)
		return
	}

	fc.n = len(fc.tokens)
}

func parseSrcFn(ctx context.Context, q *pb.Query) (*functionContext, error) {
	fnType, f := parseFuncType(q.SrcFunc) // 解析出函数类别编号fnType以及函数名（f是如allofterms的函数名）。（如果是普通的查某个谓词，那么fntype为空，f也为空）
	attr := q.Attr // 这个属性可以是函数体的用于查询的谓词属性，也可以是函数体内部的谓词
	fc := &functionContext{fnType: fnType, fname: f}
	isIndexedAttr := schema.State().IsIndexed(ctx, attr)
	var err error

	t, err := schema.State().TypeOf(attr)
	if err == nil && fnType != notAFunction && t.Name() == types.StringID.Name() {
		fc.isStringFn = true
	}

	// 下面这个大Switch是根据当前函数的fnType类别，来填充一些当前函数所需要的一些数据，并且会一些错误校验
	switch fnType {
	case notAFunction: // 如果是普通的谓词查询
		fc.n = len(q.UidList.Uids)
	case aggregatorFn:
		// confirm aggregator could apply on the attributes
		typ, err := schema.State().TypeOf(attr)
		if err != nil {
			return nil, errors.Errorf("Attribute %q is not scalar-type", x.ParseAttr(attr))
		}
		if !couldApplyAggregatorOn(f, typ) {
			return nil, errors.Errorf("Aggregator %q could not apply on %v",
				f, x.ParseAttr(attr))
		}
		fc.n = len(q.UidList.Uids)
	case compareAttrFn:
		args := q.SrcFunc.Args
		if fc.fname == eq { // Only eq can have multiple args. It should have atleast one.
			if len(args) < 1 {
				return nil, errors.Errorf("eq expects atleast 1 argument.")
			}
		} else if fc.fname == between { // between should have exactly 2 arguments.
			if len(args) != 2 {
				return nil, errors.Errorf("between expects exactly 2 argument.")
			}
		} else { // Others can have only 1 arg.
			if len(args) != 1 {
				return nil, errors.Errorf("%+v expects only 1 argument. Got: %+v",
					fc.fname, args)
			}
		}

		var tokens []string
		var ineqValues []types.Val
		// eq can have multiple args.
		for idx := 0; idx < len(args); idx++ {
			arg := args[idx]
			ineqValues = ineqValues[:0]
			ineqValue1, err := convertValue(attr, arg)
			if err != nil {
				return nil, errors.Errorf("Got error: %v while running: %v", err, q.SrcFunc)
			}
			ineqValues = append(ineqValues, ineqValue1)
			fc.eqTokens = append(fc.eqTokens, ineqValue1)

			// in case of between also pass other value.
			if fc.fname == between {
				ineqValue2, err := convertValue(attr, args[idx+1])
				if err != nil {
					return nil, errors.Errorf("Got error: %v while running: %v", err, q.SrcFunc)
				}
				idx++
				ineqValues = append(ineqValues, ineqValue2)
				fc.eqTokens = append(fc.eqTokens, ineqValue2)
			}

			if !isIndexedAttr {
				// In case of non-indexed predicate we won't have any tokens.
				continue
			}

			var lang string
			if len(q.Langs) > 0 {
				// Only one language is allowed.
				lang = q.Langs[0]
			}

			// Get tokens ge/le ineqValueToken.
			if tokens, fc.ineqValueToken, err = getInequalityTokens(ctx, q.ReadTs, attr, f, lang,
				ineqValues); err != nil {
				return nil, err
			}
			if len(tokens) == 0 {
				continue
			}
			fc.tokens = append(fc.tokens, tokens...)
		}

		// In case of non-indexed predicate, there won't be any tokens. We will fetch value
		// from data keys.
		// If number of index keys is more than no. of uids to filter, so its better to fetch values
		// from data keys directly and compare. Lets make tokens empty.
		// We don't do this for eq because eq could have multiple arguments and we would have to
		// compare the value with all of them. Also eq would usually have less arguments, hence we
		// won't be fetching many index keys.
		switch {
		case q.UidList != nil && !isIndexedAttr:
			fc.n = len(q.UidList.Uids)
		case q.UidList != nil && len(fc.tokens) > len(q.UidList.Uids) && fc.fname != eq:
			fc.tokens = fc.tokens[:0]
			fc.n = len(q.UidList.Uids)
		case q.UidList != nil && fc.fname == eq:
			if len(fc.tokens) > 0 {
				planForEqFilter(fc, attr, q.UidList.Uids)
			}
		default:
			fc.n = len(fc.tokens)
		}
	case compareScalarFn:
		argCount := 1
		if q.SrcFunc.Name == between {
			argCount = 2
		}
		if err = ensureArgsCount(q.SrcFunc, argCount); err != nil {
			return nil, err
		}
		var thresholds []int64
		for _, arg := range q.SrcFunc.Args {
			threshold, err := strconv.ParseInt(arg, 0, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "Compare %v(%v) require digits, but got invalid num",
					q.SrcFunc.Name, q.SrcFunc.Args[0])
			}
			thresholds = append(thresholds, threshold)
		}
		fc.threshold = thresholds
		checkRoot(q, fc)
	case geoFn:
		// For geo functions, we get extra information used for filtering.
		fc.tokens, fc.geoQuery, err = types.GetGeoTokens(q.SrcFunc)
		tok.EncodeGeoTokens(fc.tokens)
		if err != nil {
			return nil, err
		}
		fc.n = len(fc.tokens)
	case passwordFn:
		if err = ensureArgsCount(q.SrcFunc, 2); err != nil {
			return nil, err
		}
		fc.n = len(q.UidList.Uids)
	case standardFn, fullTextSearchFn:  // allofterms会进入到这里
		// srcfunc 0th val is func name and [2:] are args.
		// we tokenize the arguments of the query.
		// srcfnc第0个val是函数名，[2:]是参数。
		// 我们对查询的参数进行标记
		if err = ensureArgsCount(q.SrcFunc, 1); err != nil { // 验证函数的所需参数数量是否为1个
			return nil, err
		}
		required, found := verifyStringIndex(ctx, attr, fnType) // 验证函数参数的类型以及要查询的谓词是否满足条件（比如allofterms就需要index为term）
		if !found {
			return nil, errors.Errorf("Attribute %s is not indexed with type %s", x.ParseAttr(attr),
				required)
		}
		if fc.tokens, err = getStringTokens(q.SrcFunc.Args, langForFunc(q.Langs), fnType); err != nil { // tokens是一个列表，其内只包含当前函数涉及的一些变量值（包括直接字面量与变量的值）
			return nil, err
		}
		fc.intersectDest = needsIntersect(f) // 检查函数类型是否需要algo，如果函数结果需要相交，则返回true，否则返回false。
		fc.n = len(fc.tokens)
	case matchFn:
		if err = ensureArgsCount(q.SrcFunc, 2); err != nil {
			return nil, err
		}
		required, found := verifyStringIndex(ctx, attr, fnType)
		if !found {
			return nil, errors.Errorf("Attribute %s is not indexed with type %s", x.ParseAttr(attr),
				required)
		}
		fc.intersectDest = needsIntersect(f)
		// Max Levenshtein distance
		var s string
		s, q.SrcFunc.Args = q.SrcFunc.Args[1], q.SrcFunc.Args[:1]
		max, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, errors.Errorf("Levenshtein distance value must be an int, got %v", s)
		}
		if max < 0 {
			return nil, errors.Errorf("Levenshtein distance value must be greater than 0, got %v", s)
		}
		fc.threshold = []int64{max}
		fc.tokens = q.SrcFunc.Args
		fc.n = len(fc.tokens)
	case customIndexFn:
		if err = ensureArgsCount(q.SrcFunc, 2); err != nil {
			return nil, err
		}
		tokerName := q.SrcFunc.Args[0]
		if !verifyCustomIndex(ctx, q.Attr, tokerName) {
			return nil, errors.Errorf("Attribute %s is not indexed with custom tokenizer %s",
				x.ParseAttr(q.Attr), tokerName)
		}
		valToTok, err := convertValue(q.Attr, q.SrcFunc.Args[1])
		if err != nil {
			return nil, err
		}
		tokenizer, ok := tok.GetTokenizer(tokerName)
		if !ok {
			return nil, errors.Errorf("Could not find tokenizer with name %q", tokerName)
		}
		fc.tokens, _ = tok.BuildTokens(valToTok.Value,
			tok.GetTokenizerForLang(tokenizer, langForFunc(q.Langs)))
		fc.intersectDest = needsIntersect(f)
		fc.n = len(fc.tokens)
	case regexFn:
		if err = ensureArgsCount(q.SrcFunc, 2); err != nil {
			return nil, err
		}
		ignoreCase := false
		modifiers := q.SrcFunc.Args[1]
		if len(modifiers) > 0 {
			if modifiers == "i" {
				ignoreCase = true
			} else {
				return nil, errors.Errorf("Invalid regexp modifier: %s", modifiers)
			}
		}
		matchType := "(?m)" // this is cregexp library specific
		if ignoreCase {
			matchType = "(?i)" + matchType
		}
		if fc.regex, err = cregexp.Compile(matchType + q.SrcFunc.Args[0]); err != nil {
			return nil, err
		}
		fc.n = 0
	case hasFn:
		if err = ensureArgsCount(q.SrcFunc, 0); err != nil {
			return nil, err
		}
		checkRoot(q, fc)
	case similarToFn:
		if err = ensureArgsCount(q.SrcFunc, 2); err != nil {
			return nil, err
		}
		fc.vectorInfo, fc.vectorUid, err = interpretVFloatOrUid(q.SrcFunc.Args[1])
		if err != nil {
			return nil, err
		}
	case uidInFn:
		for _, arg := range q.SrcFunc.Args {
			uidParsed, err := strconv.ParseUint(arg, 0, 64)
			if err != nil {
				if e, ok := err.(*strconv.NumError); ok && e.Err == strconv.ErrSyntax {
					return nil, errors.Errorf("Value %q in %s is not a number",
						arg, q.SrcFunc.Name)
				}
				return nil, err
			}
			fc.uidsPresent = append(fc.uidsPresent, uidParsed)
		}
		sort.Slice(fc.uidsPresent, func(i, j int) bool {
			return fc.uidsPresent[i] < fc.uidsPresent[j]
		})
		checkRoot(q, fc)
		if fc.isFuncAtRoot {
			return nil, errors.Errorf("uid_in function not allowed at root")
		}
	default:
		return nil, errors.Errorf("FnType %d not handled in numFnAttrs.", fnType)
	}
	return fc, nil
}

func interpretVFloatOrUid(val string) ([]float32, uint64, error) {
	vf, err := types.ParseVFloat(val)
	if err == nil {
		return vf, 0, nil
	}
	uid, err := strconv.ParseUint(val, 0, 64)
	if err == nil {
		return nil, uid, nil
	}
	return nil, uid, errors.Errorf("Value %q is not a uid or vector", val)
}

// ServeTask is used to respond to a query.
func (w *grpcWorker) ServeTask(ctx context.Context, q *pb.Query) (*pb.Result, error) {
	ctx, span := otrace.StartSpan(ctx, "worker.ServeTask")
	defer span.End()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// It could be possible that the server isn't ready but a peer sends a
	// request. In that case we should check for the health here.
	if err := x.HealthCheck(); err != nil {
		return nil, err
	}

	gid, err := groups().BelongsToReadOnly(q.Attr, q.ReadTs)
	switch {
	case err != nil:
		return nil, err
	case gid == 0:
		return nil, errNonExistentTablet
	case gid != groups().groupId():
		return nil, errUnservedTablet
	}

	var numUids int
	if q.UidList != nil {
		numUids = len(q.UidList.Uids)
	}
	span.Annotatef(nil, "Attribute: %q NumUids: %v groupId: %v ServeTask", q.Attr, numUids, gid)

	if !groups().ServesGroup(gid) {
		return nil, errors.Errorf(
			"Temporary error, attr: %q groupId: %v Request sent to wrong server",
			x.ParseAttr(q.Attr), gid)
	}

	type reply struct {
		result *pb.Result
		err    error
	}
	c := make(chan reply, 1)
	go func() {
		result, err := processTask(ctx, q, gid)
		c <- reply{result, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case reply := <-c:
		return reply.result, reply.err
	}
}

// applyFacetsTree : we return error only when query has some problems.
// like Or has 3 arguments, argument facet val overflows integer.
// returns true if postingFacets can be included.
func applyFacetsTree(postingFacets []*api.Facet, ftree *facetsTree) (bool, error) {
	if ftree == nil {
		return true, nil
	}
	if ftree.function != nil {
		var fc *api.Facet
		for _, fci := range postingFacets {
			if fci.Key == ftree.function.key {
				fc = fci
				break
			}
		}
		if fc == nil { // facet is not there
			return false, nil
		}

		switch ftree.function.fnType {
		case compareAttrFn: // lt, gt, le, ge, eq
			fVal, err := facets.ValFor(fc)
			if err != nil {
				return false, err
			}

			v, ok := ftree.function.typesToVal[fVal.Tid]
			if !ok {
				// Not found in map and hence convert it here.
				v, err = types.Convert(ftree.function.val, fVal.Tid)
				if err != nil {
					// ignore facet if not of appropriate type.
					return false, nil
				}
			}

			return types.CompareVals(ftree.function.name, fVal, v), nil

		case standardFn: // allofterms, anyofterms
			facetType, err := facets.TypeIDFor(fc)
			if err != nil {
				return false, err
			}
			if facetType != types.StringID {
				return false, nil
			}
			return filterOnStandardFn(ftree.function.name, fc.Tokens, ftree.function.tokens)
		}
		return false, errors.Errorf("Fn %s not supported in facets filtering.", ftree.function.name)
	}

	res := make([]bool, 0, 2) // We can have max two children for a node.
	for _, c := range ftree.children {
		r, err := applyFacetsTree(postingFacets, c)
		if err != nil {
			return false, err
		}
		res = append(res, r)
	}

	// we have already checked for number of children in preprocessFilter
	switch ftree.op {
	case "not":
		return !res[0], nil
	case "and":
		return res[0] && res[1], nil
	case "or":
		return res[0] || res[1], nil
	}
	return false, errors.Errorf("Unexpected behavior in applyFacetsTree.")
}

// filterOnStandardFn : tells whether facet corresponding to fcTokens can be taken or not.
// fcTokens and argTokens should be sorted.
func filterOnStandardFn(fname string, fcTokens []string, argTokens []string) (bool, error) {
	switch fname {
	case "allofterms":
		// allofterms argTokens should be in fcTokens
		if len(argTokens) > len(fcTokens) {
			return false, nil
		}
		aidx := 0
	loop:
		for fidx := 0; aidx < len(argTokens) && fidx < len(fcTokens); {
			switch {
			case fcTokens[fidx] < argTokens[aidx]:
				fidx++
			case fcTokens[fidx] == argTokens[aidx]:
				fidx++
				aidx++
			default:
				// as all of argTokens should match
				// which is not possible now.
				break loop
			}
		}
		return aidx == len(argTokens), nil
	case "anyofterms":
		for aidx, fidx := 0, 0; aidx < len(argTokens) && fidx < len(fcTokens); {
			switch {
			case fcTokens[fidx] < argTokens[aidx]:
				fidx++
			case fcTokens[fidx] == argTokens[aidx]:
				return true, nil
			default:
				aidx++
			}
		}
		return false, nil
	}
	return false, errors.Errorf("Fn %s not supported in facets filtering.", fname)
}

type facetsFunc struct {
	name   string
	key    string
	args   []string
	tokens []string
	val    types.Val
	fnType FuncType
	// typesToVal stores converted vals of the function val for all common types. Converting
	// function val to particular type val(check applyFacetsTree()) consumes significant amount of
	// time. This maps helps in doing conversion only once(check preprocessFilter()).
	typesToVal map[types.TypeID]types.Val
}
type facetsTree struct {
	op       string
	children []*facetsTree
	function *facetsFunc
}

// commonTypeIDs is list of type ids which are more common. In preprocessFilter() we keep converted
// values for these typeIDs at every function node.
var commonTypeIDs = [...]types.TypeID{types.StringID, types.IntID, types.FloatID,
	types.DateTimeID, types.BoolID, types.DefaultID}

func preprocessFilter(tree *pb.FilterTree) (*facetsTree, error) {
	if tree == nil {
		return nil, nil
	}
	ftree := &facetsTree{}
	ftree.op = strings.ToLower(tree.Op)
	if tree.Func != nil {
		ftree.function = &facetsFunc{}
		ftree.function.key = tree.Func.Key
		ftree.function.args = tree.Func.Args

		fnType, fname := parseFuncTypeHelper(tree.Func.Name)
		if len(tree.Func.Args) != 1 {
			return nil, errors.Errorf("One argument expected in %s, but got %d.",
				fname, len(tree.Func.Args))
		}

		ftree.function.name = fname
		ftree.function.fnType = fnType

		switch fnType {
		case compareAttrFn:
			ftree.function.val = types.Val{Tid: types.StringID, Value: []byte(tree.Func.Args[0])}
			ftree.function.typesToVal = make(map[types.TypeID]types.Val, len(commonTypeIDs))
			for _, typeID := range commonTypeIDs {
				// TODO: if conversion is not possible we are not putting anything to map. In
				// applyFacetsTree we check if entry for a type is not present, we try to convert
				// it. This double conversion can be avoided.
				cv, err := types.Convert(ftree.function.val, typeID)
				if err != nil {
					continue
				}
				ftree.function.typesToVal[typeID] = cv
			}
		case standardFn:
			argTokens, aerr := tok.GetTermTokens(tree.Func.Args)
			if aerr != nil { // query error ; stop processing.
				return nil, aerr
			}
			sort.Strings(argTokens)
			ftree.function.tokens = argTokens
		default:
			return nil, errors.Errorf("Fn %s not supported in preprocessFilter.", fname)
		}
		return ftree, nil
	}

	for _, c := range tree.Children {
		ftreec, err := preprocessFilter(c)
		if err != nil {
			return nil, err
		}
		ftree.children = append(ftree.children, ftreec)
	}

	numChild := len(tree.Children)
	switch ftree.op {
	case "not":
		if numChild != 1 {
			return nil, errors.Errorf("Expected 1 child for not but got %d.", numChild)
		}
	case "and":
		if numChild != 2 {
			return nil, errors.Errorf("Expected 2 child for not but got %d.", numChild)
		}
	case "or":
		if numChild != 2 {
			return nil, errors.Errorf("Expected 2 child for not but got %d.", numChild)
		}
	default:
		return nil, errors.Errorf("Unsupported operation in facet filtering: %s.", tree.Op)
	}
	return ftree, nil
}

type countParams struct {
	readTs  uint64
	counts  []int64
	attr    string
	gid     uint32
	reverse bool   // If query is asking for ~pred
	fn      string // function name
}

func (qs *queryState) evaluate(cp countParams, out *pb.Result) error {
	countl := cp.counts[0]
	var counth int64
	if cp.fn == between {
		counth = cp.counts[1]
	}
	var illegal bool
	switch cp.fn {
	case "eq":
		illegal = countl <= 0
	case "lt":
		illegal = countl <= 1
	case "le":
		illegal = countl <= 0
	case "gt":
		illegal = countl < 0
	case "ge":
		illegal = countl <= 0
	case "between":
		illegal = countl <= 0 || counth <= 0
	default:
		x.AssertTruef(false, "unhandled count comparison fn: %v", cp.fn)
	}
	if illegal {
		return errors.Errorf("count(predicate) cannot be used to search for " +
			"negative counts (nonsensical) or zero counts (not tracked).")
	}

	countKey := x.CountKey(cp.attr, uint32(countl), cp.reverse)
	if cp.fn == "eq" {
		pl, err := qs.cache.Get(countKey)
		if err != nil {
			return err
		}
		uids, err := pl.Uids(posting.ListOptions{ReadTs: cp.readTs})
		if err != nil {
			return err
		}
		out.UidMatrix = append(out.UidMatrix, uids)
		return nil
	}

	switch cp.fn {
	case "lt":
		countl--
	case "gt":
		countl++
	}

	x.AssertTrue(countl >= 1)
	countKey = x.CountKey(cp.attr, uint32(countl), cp.reverse)

	txn := pstore.NewTransactionAt(cp.readTs, false)
	defer txn.Discard()

	pk := x.ParsedKey{Attr: cp.attr}
	itOpt := badger.DefaultIteratorOptions
	itOpt.PrefetchValues = false
	itOpt.Reverse = cp.fn == "le" || cp.fn == "lt"
	itOpt.Prefix = pk.CountPrefix(cp.reverse)

	itr := txn.NewIterator(itOpt)
	defer itr.Close()

	for itr.Seek(countKey); itr.Valid(); itr.Next() {
		item := itr.Item()
		var key []byte
		key = item.KeyCopy(key)
		k, err := x.Parse(key)
		if err != nil {
			return err
		}
		if cp.fn == between && int64(k.Count) > counth {
			break
		}

		pl, err := qs.cache.Get(item.KeyCopy(key))
		if err != nil {
			return err
		}
		uids, err := pl.Uids(posting.ListOptions{ReadTs: cp.readTs})
		if err != nil {
			return err
		}
		out.UidMatrix = append(out.UidMatrix, uids)
	}
	return nil
}

func (qs *queryState) handleHasFunction(ctx context.Context, q *pb.Query, out *pb.Result,
	srcFn *functionContext) error {
	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, "handleHasFunction")
	defer stop()
	if glog.V(3) {
		glog.Infof("handleHasFunction query: %+v\n", q)
	}

	txn := pstore.NewTransactionAt(q.ReadTs, false)
	defer txn.Discard()

	initKey := x.ParsedKey{
		Attr: q.Attr,
	}
	startKey := x.DataKey(q.Attr, q.AfterUid+1)
	prefix := initKey.DataPrefix()
	if q.Reverse {
		// Reverse does not mean reverse iteration. It means we're looking for
		// the reverse index.
		startKey = x.ReverseKey(q.Attr, q.AfterUid+1)
		prefix = initKey.ReversePrefix()
	}

	result := &pb.List{}
	var prevKey []byte
	itOpt := badger.DefaultIteratorOptions
	itOpt.PrefetchValues = false
	itOpt.AllVersions = true
	itOpt.Prefix = prefix
	it := txn.NewIterator(itOpt)
	defer it.Close()

	lang := langForFunc(q.Langs)
	needFiltering := needsStringFiltering(srcFn, q.Langs, q.Attr)

	// This function checks if we should include uid in result or not when has is queried with
	// @lang(eg: has(name@en)). We need to do this inside this function to return correct result
	// for first.
	checkInclusion := func(uid uint64) error {
		if !needFiltering {
			return nil
		}

		_, err := qs.getValsForUID(q.Attr, lang, uid, q.ReadTs)
		return err
	}

	cnt := int32(0)
loop:
	// This function could be switched to the stream.Lists framework, but after the change to use
	// BitCompletePosting, the speed here is already pretty fast. The slowdown for @lang predicates
	// occurs in filterStringFunction (like has(name) queries).
	for it.Seek(startKey); it.Valid(); {
		item := it.Item()
		if bytes.Equal(item.Key(), prevKey) {
			it.Next()
			continue
		}
		prevKey = append(prevKey[:0], item.Key()...)

		// Parse the key upfront, otherwise ReadPostingList would advance the
		// iterator.
		pk, err := x.Parse(item.Key())
		if err != nil {
			return err
		}

		if pk.HasStartUid {
			// The keys holding parts of a split key should not be accessed here because
			// they have a different prefix. However, the check is being added to guard
			// against future bugs.
			continue
		}

		// The following optimization speeds up this iteration considerably, because it avoids
		// the need to run ReadPostingList.
		if item.UserMeta()&posting.BitEmptyPosting > 0 {
			// This is an empty posting list. So, it should not be included.
			continue
		}
		if item.UserMeta()&posting.BitCompletePosting > 0 {
			// This bit would only be set if there are valid uids in UidPack.
			err := checkInclusion(pk.Uid)
			switch {
			case err == posting.ErrNoValue:
				continue
			case err != nil:
				return err
			}
			// skip entries upto Offset and do not store in the result.
			if cnt < q.Offset {
				cnt++
				continue
			}
			result.Uids = append(result.Uids, pk.Uid)

			// We'll stop fetching if we fetch the required count.
			if len(result.Uids) >= int(q.First) {
				break
			}
			continue
		}

		// We do need to copy over the key for ReadPostingList.
		l, err := posting.ReadPostingList(item.KeyCopy(nil), it)
		if err != nil {
			return err
		}
		empty, err := l.IsEmpty(q.ReadTs, 0)
		switch {
		case err != nil:
			return err
		case !empty:
			err := checkInclusion(pk.Uid)
			switch {
			case err == posting.ErrNoValue:
				continue
			case err != nil:
				return err
			}
			// skip entries upto Offset and do not store in the result.
			if cnt < q.Offset {
				cnt++
				continue
			}
			result.Uids = append(result.Uids, pk.Uid)

			// We'll stop fetching if we fetch the required count.
			if len(result.Uids) >= int(q.First) {
				break loop
			}
		}

		if len(result.Uids)%100000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}
	if span != nil {
		span.Annotatef(nil, "handleHasFunction found %d uids", len(result.Uids))
	}
	out.UidMatrix = append(out.UidMatrix, result)
	return nil
}
