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

package posting

import (
	"bytes"
	"context"
	"encoding/hex"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	ostats "go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/badger/v4"
	bpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/hypermodeinc/dgraph/v24/protos/pb"
	"github.com/hypermodeinc/dgraph/v24/x"
)

type pooledKeys struct {
	// keysCh is populated with batch of 64 keys that needs to be rolled up during reads
	keysCh chan *[][]byte
	// keysPool is sync.Pool to share the batched keys to rollup.
	keysPool *sync.Pool
}

// incrRollupi is used to batch keys for rollup incrementally.
type incrRollupi struct {
	// We are using 2 priorities with now, idx 0 represents the high priority keys to be rolled
	// up while idx 1 represents low priority keys to be rolled up.
	priorityKeys []*pooledKeys
	count        uint64

	// Get Timestamp function gets a new timestamp to store the rollup at. This makes sure that
	// we are not overwriting any transaction. If there are transactions that are ongoing,
	// which modify the item, rollup wouldn't affect the data, as a delta would be written
	// later on
	getNewTs func(bool) uint64
	closer   *z.Closer
}

type CachePL struct {
	list       *List
	lastUpdate uint64
}

var (
	// ErrTsTooOld is returned when a transaction is too old to be applied.
	ErrTsTooOld = errors.Errorf("Transaction is too old")
	// ErrInvalidKey is returned when trying to read a posting list using
	// an invalid key (e.g the key to a single part of a larger multi-part list).
	ErrInvalidKey = errors.Errorf("cannot read posting list using multi-part list key")
	// ErrHighPriorityOp is returned when rollup is cancelled so that operations could start.
	ErrHighPriorityOp = errors.New("Cancelled rollup to make way for high priority operation")

	// IncrRollup is used to batch keys for rollup incrementally.
	IncrRollup = &incrRollupi{
		priorityKeys: make([]*pooledKeys, 2),
	}
)

var memoryLayer *MemoryLayer

func init() {
	x.AssertTrue(len(IncrRollup.priorityKeys) == 2)
	for i := range IncrRollup.priorityKeys {
		IncrRollup.priorityKeys[i] = &pooledKeys{
			keysCh: make(chan *[][]byte, 16),
			keysPool: &sync.Pool{
				New: func() interface{} {
					return new([][]byte)
				},
			},
		}
	}
}

// rollUpKey takes the given key's posting lists, rolls it up and writes back to badger
func (ir *incrRollupi) rollUpKey(writer *TxnWriter, key []byte) error {
	// Get a new non read only ts. This makes sure that no other txn would write at this
	// ts, overwriting some data. Wait to read the Posting list until ts-1 have been applied
	// to badger. This helps us prevent issues with wal replay, as we now have a timestamp
	// where nothing was writen to dgraph.
	ts := ir.getNewTs(false)

	// Get a wait channel from oracle. Can't use WaitFromTs as we also need to check if other
	// operations need to start. If ok is not true, that means we have already passed the ts,
	// and we don't need to wait.
	waitCh, ok := o.addToWaiters(ts)
	if ok {
		select {
		case <-ir.closer.HasBeenClosed():
			return ErrHighPriorityOp

		case <-waitCh:
		}
	}

	l, err := GetNoStore(key, ts)
	if err != nil {
		return err
	}

	kvs, err := l.Rollup(nil, ts)
	if err != nil {
		return err
	}

	RemoveCacheFor(key)
	memoryLayer.del(key)
	// TODO Update cache with rolled up results
	// If we do a rollup, we typically won't need to update the key in cache.
	// The only caveat is that the key written by rollup would be written at +1
	// timestamp, hence bumping the latest TS for the key by 1. The cache should
	// understand that.
	const N = uint64(1000)
	if glog.V(2) {
		if count := atomic.AddUint64(&ir.count, 1); count%N == 0 {
			glog.V(2).Infof("Rolled up %d keys", count)
		}
	}
	return writer.Write(&bpb.KVList{Kv: kvs})
}

// TODO: When the opRollup is not running the keys from keysPool of ir are dropped. Figure out some
// way to handle that.
func (ir *incrRollupi) addKeyToBatch(key []byte, priority int) {
	rki := ir.priorityKeys[priority]
	batch := rki.keysPool.Get().(*[][]byte)
	*batch = append(*batch, key)
	if len(*batch) < 16 {
		rki.keysPool.Put(batch)
		return
	}

	select {
	case rki.keysCh <- batch:
	default:
		// Drop keys and build the batch again. Lossy behavior.
		*batch = (*batch)[:0]
		rki.keysPool.Put(batch)
	}
}

// Process will rollup batches of 64 keys in a go routine.
func (ir *incrRollupi) Process(closer *z.Closer, getNewTs func(bool) uint64) {
	ir.getNewTs = getNewTs
	ir.closer = closer

	defer closer.Done()

	writer := NewTxnWriter(pstore)
	defer writer.Flush()

	m := make(map[uint64]int64) // map hash(key) to ts. hash(key) to limit the size of the map.
	limiter := time.NewTicker(time.Millisecond)
	defer limiter.Stop()
	cleanupTick := time.NewTicker(5 * time.Minute)
	defer cleanupTick.Stop()
	forceRollupTick := time.NewTicker(500 * time.Millisecond)
	defer forceRollupTick.Stop()

	doRollup := func(batch *[][]byte, priority int) {
		currTs := time.Now().Unix()
		for _, key := range *batch {
			hash := z.MemHash(key)
			if elem := m[hash]; currTs-elem >= 10 {
				// Key not present or Key present but last roll up was more than 2 sec ago.
				// Add/Update map and rollup.
				m[hash] = currTs
				if err := ir.rollUpKey(writer, key); err != nil {
					glog.Warningf("Error %v rolling up key %v\n", err, key)
				}
			}
		}
		*batch = (*batch)[:0]
		ir.priorityKeys[priority].keysPool.Put(batch)
	}

	for {
		select {
		case <-closer.HasBeenClosed():
			return
		case <-cleanupTick.C:
			currTs := time.Now().Unix()
			for hash, ts := range m {
				// Remove entries from map which have been there for there more than 10 seconds.
				if currTs-ts >= 10 {
					delete(m, hash)
				}
			}
		case <-forceRollupTick.C:
			batch := ir.priorityKeys[0].keysPool.Get().(*[][]byte)
			if len(*batch) > 0 {
				doRollup(batch, 0)
			} else {
				ir.priorityKeys[0].keysPool.Put(batch)
			}
		case batch := <-ir.priorityKeys[0].keysCh:
			doRollup(batch, 0)
			// We don't need a limiter here as we don't expect to call this function frequently.
		case batch := <-ir.priorityKeys[1].keysCh:
			doRollup(batch, 1)
			// throttle to 1 batch = 16 rollups per 1 ms.
			<-limiter.C
		}
	}
}

// ShouldAbort returns whether the transaction should be aborted.
func (txn *Txn) ShouldAbort() bool {
	if txn == nil {
		return false
	}
	return atomic.LoadUint32(&txn.shouldAbort) > 0
}

func (txn *Txn) addConflictKey(conflictKey uint64) {
	txn.Lock()
	defer txn.Unlock()
	if txn.conflicts == nil {
		txn.conflicts = make(map[uint64]struct{})
	}
	if conflictKey > 0 {
		txn.conflicts[conflictKey] = struct{}{}
	}
}

// FillContext updates the given transaction context with data from this transaction.
func (txn *Txn) FillContext(ctx *api.TxnContext, gid uint32, isErrored bool) {
	txn.Lock()
	ctx.StartTs = txn.StartTs

	for key := range txn.conflicts {
		// We don'txn need to send the whole conflict key to Zero. Solving #2338
		// should be done by sending a list of mutating predicates to Zero,
		// along with the keys to be used for conflict detection.
		fps := strconv.FormatUint(key, 36)
		ctx.Keys = append(ctx.Keys, fps)
	}
	ctx.Keys = x.Unique(ctx.Keys)

	txn.Unlock()
	// If the trasnaction has errored out, we don't need to update it, as these values will never be read.
	// Sometimes, the transaction might have failed due to timeout. If we let this trasnactino update, there
	// could be deadlock with the running transaction.
	if !isErrored {
		txn.Update()
	}
	txn.cache.fillPreds(ctx, gid)
}

// CommitToDisk commits a transaction to disk.
// This function only stores deltas to the commit timestamps. It does not try to generate a state.
// State generation is done via rollups, which happen when a snapshot is created.
// Don't call this for schema mutations. Directly commit them.
func (txn *Txn) CommitToDisk(writer *TxnWriter, commitTs uint64) error {
	if commitTs == 0 {
		return nil
	}

	cache := txn.cache
	cache.Lock()
	defer cache.Unlock()

	var keys []string
	for key := range cache.deltas {
		keys = append(keys, key)
	}

	defer func() {
		// Add these keys to be rolled up after we're done writing. This is the right place for them
		// to be rolled up, because we just pushed these deltas over to Badger.
		for _, key := range keys {
			IncrRollup.addKeyToBatch([]byte(key), 1)
		}
	}()

	var idx int
	for idx < len(keys) {
		// writer.update can return early from the loop in case we encounter badger.ErrTxnTooBig. On
		// that error, writer.update would still commit the transaction and return any error. If
		// nil, we continue to process the remaining keys.
		err := writer.update(commitTs, func(btxn *badger.Txn) error {
			for ; idx < len(keys); idx++ {
				key := keys[idx]
				data := cache.deltas[key]
				if len(data) == 0 {
					continue
				}
				if ts := cache.maxVersions[key]; ts >= commitTs {
					// Skip write because we already have a write at a higher ts.
					// Logging here can cause a lot of output when doing Raft log replay. So, let's
					// not output anything here.
					continue
				}
				err := btxn.SetEntry(&badger.Entry{
					Key:      []byte(key),
					Value:    data,
					UserMeta: BitDeltaPosting,
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func ResetCache() {
	memoryLayer.clear()
}

// RemoveCacheFor will delete the list corresponding to the given key.
func RemoveCacheFor(key []byte) {
	memoryLayer.del(key)
}

type Cache struct {
	data *ristretto.Cache[[]byte, *CachePL]

	numCacheRead      atomic.Int64
	numCacheReadFails atomic.Int64
	numCacheSave      atomic.Int64
}

func (c *Cache) wait() {
	if c == nil {
		return
	}
	c.data.Wait()
}

func (c *Cache) get(key []byte) (*CachePL, bool) {
	if c == nil {
		return nil, false
	}
	val, ok := c.data.Get(key)
	if !ok {
		c.numCacheReadFails.Add(1)
		return val, ok
	}
	if val.list == nil {
		c.numCacheReadFails.Add(1)
		return nil, false
	}
	c.numCacheRead.Add(1)
	return val, true
}

func (c *Cache) set(key []byte, i *CachePL) {
	if c == nil {
		return
	}
	c.numCacheSave.Add(1)
	c.data.Set(key, i, 1)
}

func (c *Cache) del(key []byte) {
	if c == nil {
		return
	}
	c.data.Del(key)
}

func (c *Cache) clear() {
	if c == nil {
		return
	}
	c.data.Clear()
}

type MemoryLayer struct {
	// config
	removeOnUpdate bool

	// data
	cache *Cache

	// metrics
	statsHolder *StatsHolder
}

func (ml *MemoryLayer) clear() {
	ml.cache.clear()
}
func (ml *MemoryLayer) del(key []byte) {
	ml.cache.del(key)
}

func GetStatsHolder() *StatsHolder {
	return memoryLayer.statsHolder
}

func initMemoryLayer(cacheSize int64, removeOnUpdate bool) *MemoryLayer {
	ml := &MemoryLayer{}
	ml.removeOnUpdate = removeOnUpdate
	ml.statsHolder = NewStatsHolder()
	if cacheSize > 0 {
		cache, err := ristretto.NewCache[[]byte, *CachePL](&ristretto.Config[[]byte, *CachePL]{
			// Use 5% of cache memory for storing counters.
			NumCounters: int64(float64(cacheSize) * 0.05 * 2),
			MaxCost:     int64(float64(cacheSize) * 0.95),
			BufferItems: 16,
			Metrics:     true,
			Cost: func(val *CachePL) int64 {
				return 1
			},
			ShouldUpdate: func(cur, prev *CachePL) bool {
				return !(cur.list != nil && prev.list != nil && prev.list.maxTs > cur.list.maxTs)
			},
		})
		x.Check(err)
		go func() {
			m := cache.Metrics
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				// Record the posting list cache hit ratio
				ostats.Record(context.Background(), x.PLCacheHitRatio.M(m.Ratio()))

				x.NumPostingListCacheSave.M(ml.cache.numCacheRead.Load())
				ml.cache.numCacheSave.Store(0)

				x.NumPostingListCacheRead.M(ml.cache.numCacheRead.Load())
				ml.cache.numCacheRead.Store(0)

				x.NumPostingListCacheReadFail.M(ml.cache.numCacheReadFails.Load())
				ml.cache.numCacheReadFails.Store(0)
			}
		}()

		ml.cache = &Cache{data: cache}
	}
	return ml
}

func NewCachePL() *CachePL {
	return &CachePL{
		list: nil,
	}
}

func checkForRollup(key []byte, l *List) {
	deltaCount := l.mutationMap.len()
	// If deltaCount is high, send it to high priority channel instead.
	if deltaCount > 500 {
		IncrRollup.addKeyToBatch(key, 0)
	}
}

func (ml *MemoryLayer) wait() {
	ml.cache.wait()
}

func (ml *MemoryLayer) updateItemInCache(key string, delta []byte, startTs, commitTs uint64) {
	if commitTs == 0 {
		return
	}

	if ml.removeOnUpdate {
		// TODO We should mark the key as deleted instead of directly deleting from the cache.
		ml.del([]byte(key))
		return
	}

	val, ok := ml.cache.get([]byte(key))
	if !ok {
		return
	}

	val.lastUpdate = commitTs

	if val.list != nil {
		p := new(pb.PostingList)
		x.Check(proto.Unmarshal(delta, p))

		if p.Pack == nil {
			val.list.setMutationAfterCommit(startTs, commitTs, p, true)
			checkForRollup([]byte(key), val.list)
		} else {
			// Data was rolled up. TODO figure out how is UpdateCachedKeys getting delta which is pack)
			ml.del([]byte(key))
		}

	}
}

// RemoveCachedKeys will delete the cached list by this txn.
func (txn *Txn) UpdateCachedKeys(commitTs uint64) {
	if txn == nil || txn.cache == nil {
		return
	}

	memoryLayer.wait()
	for key, delta := range txn.cache.deltas {
		memoryLayer.updateItemInCache(key, delta, txn.StartTs, commitTs)
	}
}

func unmarshalOrCopy(plist *pb.PostingList, item *badger.Item) error {
	if plist == nil {
		return errors.Errorf("cannot unmarshal value to a nil posting list of key %s",
			hex.Dump(item.Key()))
	}

	return item.Value(func(val []byte) error {
		if len(val) == 0 {
			// empty pl
			return nil
		}
		return proto.Unmarshal(val, plist) // 反序列化badger中KV对的Value字符切片，并且将数据自动填充到plist对象内部
	})
}

// ReadPostingList constructs the posting list from the disk using the passed iterator.
// Use forward iterator with allversions enabled in iter options.
// key would now be owned by the posting list. So, ensure that it isn't reused elsewhere.
// ReadPostingList使用传递的迭代器从磁盘构造发布列表。在iter选项中启用所有版本的情况下使用正向迭代器。key现在将归发布列表所有。因此，请确保它不会在其他地方重复使用。
func ReadPostingList(key []byte, it *badger.Iterator) (*List, error) {
	// Previously, ReadPostingList was not checking that a multi-part list could only
	// be read via the main key. This lead to issues during rollup because multi-part
	// lists ended up being rolled-up multiple times. This issue was caught by the
	// uid-set Jepsen test.
	// 以前，ReadPostingList没有检查多部分列表是否只能通过主键读取。这会导致汇总过程中出现问题，因为多部分列表最终会被多次汇总。uid集Jepsen测试发现了此问题。
	pk, err := x.Parse(key) // 解析key，只用于下面的的非法判断，去badger查值还是用key
	if err != nil {
		return nil, errors.Wrapf(err, "while reading posting list with key [%v]", key)
	}

	start := time.Now()
	defer func() {
		ms := x.SinceMs(start)
		var tags []tag.Mutator
		tags = append(tags, tag.Upsert(x.KeyMethod, "iterate"))
		tags = append(tags, tag.Upsert(x.KeyStatus, pk.Attr))
		_ = ostats.RecordWithTags(context.Background(), tags, x.BadgerReadLatencyMs.M(ms))
	}()

	if pk.HasStartUid {
		// Trying to read a single part of a multi part list. This type of list
		// should be read using using the main key because the information needed
		// to access the whole list is stored there.
		// The function returns a nil list instead. This is safe to do because all
		// public methods of the List object are no-ops and the list is being already
		// accessed via the main key in the places where this code is reached (e.g rollups).
		return nil, ErrInvalidKey
	}

	l := new(List)
	l.key = key //设置本PostingList的Key
	l.plist = new(pb.PostingList) //new一个不可变层
	l.minTs = 0

	// We use the following block of code to trigger incremental rollup on this key.//我们使用以下代码块来触发此key的增量汇总。
	deltaCount := 0
	defer func() {
		if deltaCount > 0 {
			// If deltaCount is high, send it to high priority channel instead. //如果deltaCount为高，则将其发送到高优先级通道。
			if deltaCount > 500 {
				IncrRollup.addKeyToBatch(key, 0)
			} else {
				IncrRollup.addKeyToBatch(key, 1)
			}
		}
	}()

	// Iterates from highest Ts to lowest Ts //从最高版本Ts迭代到最低版本Ts（每一个item对应一个kv对）
	for it.Valid() {
		item := it.Item()
		if !bytes.Equal(item.Key(), l.key) {
			break
		}
		l.maxTs = x.Max(l.maxTs, item.Version()) // 设置在本List的最大版本号
		if item.IsDeletedOrExpired() { // 如果当前entry过期了或者被删除了，直接跳过
			// Don't consider any more versions.
			break
		}

		switch item.UserMeta() { // UserMeta返回用户设置的UserMeta。通常，这个字节（可选地由用户设置）用于解释值。应该是Dgraph在执行突变时自己设置的
			case BitEmptyPosting: // 遍历到为空的了，就直接返回即可
				return l, nil
			case BitCompletePosting: // 已经是完整全部的目标posting了，直接可以跳出当前函数返回了（即遍历到了不可变层的那个kv对）
				if err := unmarshalOrCopy(l.plist, item); err != nil { // NOTE:核心操作，反序列化value，并将相应的值赋给plist,注意目标UID在List.plist.Pack.Blocks中
					return nil, err
				}

				l.minTs = item.Version() // 设置不可变层的commitTS
				// No need to do Next here. The outer loop can take care of skipping
				// more versions of the same key.
				// 这里不需要做下一步。外循环可以跳过同一密钥的更多版本。
				return l, nil
			case BitDeltaPosting: // 当前遍历到了增量（即可变层的数据），注意增量是可以有多个的（即每个增量对应一个kv对）
				err := item.Value(func(val []byte) error {
					pl := &pb.PostingList{}
					if err := proto.Unmarshal(val, pl); err != nil {
						return err
					}
					pl.CommitTs = item.Version()
					if l.mutationMap == nil { // 若可变层对象为空，那么就新建一个可变层对象
						l.mutationMap = newMutableLayer()
					}
					l.mutationMap.insertCommittedPostings(pl) // 加入可变层对象中
					return nil
				})
				if err != nil {
					return nil, err
				}
				deltaCount++ // 计数++
			case BitSchemaPosting: //读到了schema，这里不能读到schema，有错误出现！！！
				return nil, errors.Errorf(
					"Trying to read schema in ReadPostingList for key: %s", hex.Dump(key))
			default:
				return nil, errors.Errorf(
					"Unexpected meta: %d for key: %s", item.UserMeta(), hex.Dump(key))
		}
		if item.DiscardEarlierVersions() { // DiscardEarlierVersions返回item是否在创建时具有在有多个可用密钥时丢弃早期版本的选项。
			break
		}
		it.Next() // 指标下移
	}
	return l, nil
}

func copyList(l *List) *List {
	l.AssertRLock()
	// No need to clone the immutable layer or the key since mutations will not modify it.
	// 不需要克隆不可变层或key，因为突变不会修改它。
	lCopy := &List{  // 非深度克隆
		minTs: l.minTs,
		maxTs: l.maxTs,
		key:   l.key,
		plist: l.plist, 
	}
	lCopy.mutationMap = l.mutationMap.clone() // 可变层的深度克隆
	return lCopy
}

func (c *CachePL) Set(l *List, readTs uint64) {
	if c.lastUpdate < readTs && (c.list == nil || c.list.maxTs < l.maxTs) {
		c.list = l
	}
}

func (ml *MemoryLayer) readFromCache(key []byte, readTs uint64) *List {
	cacheItem, ok := ml.cache.get(key) // NOTE:核心操作，ml是全局的内存层对象

	if ok && cacheItem.list != nil && cacheItem.list.minTs <= readTs { // 判断版本，如果本次读取的readTs大于minTs
		cacheItem.list.RLock()
		lCopy := copyList(cacheItem.list)
		cacheItem.list.RUnlock()
		checkForRollup(key, lCopy)
		return lCopy
	}
	return nil
}

// NOTE:Dgraph与Badger的连接点，这个函数里面就主要都是Badger的一些方法调用，得到迭代器对象后，再由ReadPostingList对目标数据整合得到Dgraph的PostingList对象
func (ml *MemoryLayer) readFromDisk(key []byte, pstore *badger.DB, readTs uint64) (*List, error) {
	txn := pstore.NewTransactionAt(readTs, false) // 这个函数是badger托管模式下才会用到的函数（此时可以自定义ReadTs的值，即将时间戳的控制权交给了Dgraph）
	defer txn.Discard()

	// When we do rollups, an older version would go to the top of the LSM tree, which can cause
	// issues during txn.Get. Therefore, always iterate.
	// 当我们进行汇总时，旧版本将转到LSM树的顶部，这可能会在txn期间导致问题。得到。因此，始终迭代。
	iterOpts := badger.DefaultIteratorOptions
	iterOpts.AllVersions = true
	iterOpts.PrefetchValues = false
	itr := txn.NewKeyIterator(key, iterOpts)
	// NewKeyIterator与在看Badger源码的NewIterator类似，但允许用户迭代单个键的所有版本。在内部，它在提供的opt中设置Prefix选项，并在从LSM树中拾取表之前使用该前缀额外运行布隆过滤器查找。
	defer itr.Close()
	itr.Seek(key) //如果存在，Seek将查找提供的密钥。如果不存在，如果向前迭代，它将寻求比提供的键大的下一个最小键。如果向后迭代，行为将发生逆转。
	l, err := ReadPostingList(key, itr) // NOTE:核心操作，使用迭代器构造DGraph的PostingList对象
	if err != nil {
		return l, err
	}
	return l, nil
}

// Saves the data in the cache. The caller must ensure that the list provided is the latest possible.
// 将数据保存在缓存中。调用者必须确保提供的列表是最新的。
func (ml *MemoryLayer) saveInCache(key []byte, l *List) {
	l.RLock()
	defer l.RUnlock()
	cacheItem := NewCachePL()
	cacheItem.list = copyList(l)
	cacheItem.lastUpdate = l.maxTs
	ml.cache.set(key, cacheItem) // 设置缓存
}

// 下面这个函数主要做的就是依次在缓存、磁盘中找或者new出目标PostingList对象，需要特别注意的是，下面设置的readTs只是一个有关本次查询的readTs（与Badger的ReadTs在查询流程上一个东西，一个值），而MinTs则是该类数据的可变层与不可变层分界线TS（貌似不可变层就是一个单独的KV对）。
func (ml *MemoryLayer) ReadData(key []byte, pstore *badger.DB, readTs uint64) (*List, error) {
	// We first try to read the data from cache, if it is present. If it's not present, then we would read the
	// latest data from the disk. This would get stored in the cache. If this read has a minTs > readTs then
	// we would have to read the correct timestamp from the disk.
	// 我们首先尝试从缓存中读取数据（如果存在）。如果它不存在，那么我们将从磁盘读取最新数据。这将被存储在缓存中。如果此读取具有minTs>readTs，那么我们必须从磁盘读取正确的时间戳（minTs是不变层时间戳）。
	l := ml.readFromCache(key, readTs) // NOTE:核心操作，先尝试从Cache中读取PostList
	// zzlTODO:大部分都看懂了，但还有一个待解决的点，就是为什么下面先读取了所有版本，然后才会再去读当前查询的版本呢？以及key值应该是不相同把，相同的话不怕Badger的合并给合并了？
	if l != nil { // 如果读出来数据了，就直接跳出来
		l.mutationMap.setTs(readTs) // 设置读出来的PostList对象的可变层的readTs
		return l, nil
	}
	l, err := ml.readFromDisk(key, pstore, math.MaxUint64) // NOTE:核心操作，尝试从磁盘中读取（readTs设置为最大值，代表读取所有版本）
	if err != nil {
		return nil, err
	}
	ml.saveInCache(key, l) // 将刚从磁盘读取的PostList，保存到缓存中
	if l.minTs == 0 || readTs >= l.minTs { // 如果当前l的不可变层对象无目标数据，或者当次查询的readTs大于等于不可变层KV对的CommitTs
		l.mutationMap.setTs(readTs)
		return l, nil
	}

	l, err = ml.readFromDisk(key, pstore, readTs) // NOTE:核心操作，尝试从磁盘中读取（readTs设置为本次查询的readTs）
	if err != nil {
		return nil, err
	}

	l.mutationMap.setTs(readTs)
	return l, nil
}

func GetNew(key []byte, pstore *badger.DB, readTs uint64) (*List, error) {
	return getNew(key, pstore, readTs)
}


// NOTE:202506054 事物时间戳返回流程
// Zero节点的Timestamps函数接口返回 -> s.needTs(s是ServerState对象，保存Dgraph数据库的状态，needTs是长度为100的通道，具体处理在NOTE:2025060502) -> 
// posting.Oracle().MaxAssigned() 或者 worker.State.GetTimestamp（去zero请求时间戳）  -> 
// qc.req.StartTs（qc是queryContext对象，保存处理请求的所需的变量） -> req.ReadTs（req是query.Request对象） -> sg.ReadTs（sg是查询子图，取自req.Subgraphs[idx]） -> 
// q.ReadTs（q是查询任务对象pb.Query，在NOTE:2025060500创建） -> qs.cache.startTs（qs是查询状态缓存的queryState，在NOTE:202506051创建） -> readTs
// NOTE:重点：所以实际上readTs就是由向Zero节点请求的StartTs简单转换的，二者是一个东西
// 除了上面的对象，还有一个查询函数对象functionContext很重要，这个主要是保存诸如has等函数的一些信息
func getNew(key []byte, pstore *badger.DB, readTs uint64) (*List, error) { 
	if pstore.IsClosed() {
		return nil, badger.ErrDBClosed
	}

	l, err := memoryLayer.ReadData(key, pstore, readTs) //NOTE:核心操作，读取目标数据，先看缓存，再看磁盘（memoryLayer是个全局单例内存层对象）
	if err != nil {
		return l, err
	}
	return l, nil
}
