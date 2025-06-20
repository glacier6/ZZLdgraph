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

package worker

import (
	"context"
	"math"
	"os"
	"time"

	"github.com/golang/glog"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/hypermodeinc/dgraph/v24/protos/pb"
	"github.com/hypermodeinc/dgraph/v24/raftwal"
	"github.com/hypermodeinc/dgraph/v24/x"
)

const (
	// NOTE: SuperFlag defaults must include every possible option that can be used. This way, if a
	//       user makes a typo while defining a SuperFlag we can catch it and fail right away rather
	//       than fail during runtime while trying to retrieve an option that isn't there.
	//
	//       For easy readability, keep the options without default values (if any) at the end of
	//       the *Defaults string. Also, since these strings are printed in --help text, avoid line
	//       breaks.
	AuditDefaults  = `compress=false; days=10; size=100; dir=; output=; encrypt-file=;`
	BadgerDefaults = `compression=snappy; numgoroutines=8;`
	RaftDefaults   = `learner=false; snapshot-after-entries=10000; ` +
		`snapshot-after-duration=30m; pending-proposals=256; idx=; group=;`
	SecurityDefaults = `token=; whitelist=;`
	CDCDefaults      = `file=; kafka=; sasl_user=; sasl_password=; ca_cert=; client_cert=; ` +
		`client_key=; sasl-mechanism=PLAIN; tls=false;`
	LimitDefaults = `mutations=allow; query-edge=1000000; normalize-node=10000; ` +
		`mutations-nquad=1000000; disallow-drop=false; query-timeout=0ms; txn-abort-after=5m; ` +
		` max-retries=10;max-pending-queries=10000;shared-instance=false;type-filter-uid-limit=10`
	ZeroLimitsDefaults = `uid-lease=0; refill-interval=30s; disable-admin-http=false;`
	GraphQLDefaults    = `introspection=true; debug=false; extensions=true; poll-interval=1s; ` +
		`lambda-url=;`
	CacheDefaults        = `size-mb=1024; percentage=40,40,20; delete-on-updates=true`
	FeatureFlagsDefaults = `normalize-compatibility-mode=`
)

// ServerState holds the state of the Dgraph server.
// ServerState保存了Dgraph服务器的状态。
type ServerState struct {
	FinishCh chan struct{} // channel to wait for all pending reqs to finish.

	Pstore   *badger.DB // posting list存储对象（即badger）
	WALstore *raftwal.DiskStorage // WALstore存储对象（即RAFT的那个日志）
	gcCloser *z.Closer // closer for valueLogGC

	needTs chan tsReq // 待请求时间戳队列
}

// State is the instance of ServerState used by the current server.
//State是当前服务器使用的ServerState的单一实例对象，保存在woker.State。
var State ServerState

// InitServerState initializes this server's state.
//InitServerState初始化此服务器的状态。
func InitServerState() {
	Config.validate()

	State.FinishCh = make(chan struct{})
	State.needTs = make(chan tsReq, 100) // 创建待请求时间戳通道

	State.InitStorage()
	go State.fillTimestampRequests()

	groupId, err := x.ReadGroupIdFile(Config.PostingDir)
	if err != nil {
		glog.Warningf("Could not read %s file inside posting directory %s.", x.GroupIdFileName,
			Config.PostingDir)
	}
	x.WorkerConfig.ProposedGroupId = groupId
}

func setBadgerOptions(opt badger.Options) badger.Options {
	opt = opt.WithSyncWrites(false).
		WithLogger(&x.ToGlog{}).
		WithEncryptionKey(x.WorkerConfig.EncryptionKey)

	// Disable conflict detection in badger. Alpha runs in managed mode and
	// perform its own conflict detection so we don't need badger's conflict
	// detection. Using badger's conflict detection uses memory which can be
	// saved by disabling it.
	opt.DetectConflicts = false

	// Settings for the data directory.
	return opt
}

func (s *ServerState) InitStorage() {
	var err error

	if x.WorkerConfig.EncryptionKey != nil {
		// non-nil key file
		if !EnterpriseEnabled() {
			// not licensed --> crash.
			glog.Fatal("Valid Enterprise License needed for the Encryption feature.")
		} else {
			// licensed --> OK.
			glog.Infof("Encryption feature enabled.")
		}
	}

	{
		// Write Ahead Log directory
		x.Checkf(os.MkdirAll(Config.WALDir, 0700), "Error while creating WAL dir.")
		s.WALstore, err = raftwal.InitEncrypted(Config.WALDir, x.WorkerConfig.EncryptionKey)
		x.Check(err)
	}
	{
		// Postings directory
		// All the writes to posting store should be synchronous. We use batched writers
		// for posting lists, so the cost of sync writes is amortized.
		x.Check(os.MkdirAll(Config.PostingDir, 0700))
		opt := x.WorkerConfig.Badger.
			WithDir(Config.PostingDir).WithValueDir(Config.PostingDir).
			WithNumVersionsToKeep(math.MaxInt32).
			WithNamespaceOffset(x.NamespaceOffset)
		opt = setBadgerOptions(opt)

		// Print the options w/o exposing key.
		// TODO: Build a stringify interface in Badger options, which is used to print nicely here.
		key := opt.EncryptionKey
		opt.EncryptionKey = nil
		glog.Infof("Opening postings BadgerDB with options: %+v\n", opt)
		opt.EncryptionKey = key

		s.Pstore, err = badger.OpenManaged(opt)
		x.Checkf(err, "Error while creating badger KV posting store")

		// zero out from memory
		opt.EncryptionKey = nil
	}
	// Temp directory
	x.Check(os.MkdirAll(x.WorkerConfig.TmpDir, 0700))

	s.gcCloser = z.NewCloser(3)
	go x.RunVlogGC(s.Pstore, s.gcCloser)
	// Commenting this out because Badger is doing its own cache checks.
	go x.MonitorCacheHealth(s.Pstore, s.gcCloser)
	go x.MonitorDiskMetrics("postings_fs", Config.PostingDir, s.gcCloser)
}

// Dispose stops and closes all the resources inside the server state.
func (s *ServerState) Dispose() {
	s.gcCloser.SignalAndWait()
	if err := s.Pstore.Close(); err != nil {
		glog.Errorf("Error while closing postings store: %v", err)
	}
	if err := s.WALstore.Close(); err != nil {
		glog.Errorf("Error while closing WAL store: %v", err)
	}
}

func (s *ServerState) GetTimestamp(readOnly bool) uint64 {
	tr := tsReq{readOnly: readOnly, ch: make(chan uint64)}
	s.needTs <- tr // 将当前待获得时间戳的结构体压入worker的ServerState的needTs队列里面
	return <-tr.ch // 等到异步获取得到了后，将其获取的txn时间戳返回
}

func (s *ServerState) fillTimestampRequests() {
	const (
		initDelay = 10 * time.Millisecond
		maxDelay  = time.Second
	)

	defer func() {
		glog.Infoln("Exiting fillTimestampRequests")
	}()

	var reqs []tsReq
	for {
		// Reset variables.
		reqs = reqs[:0]
		delay := initDelay

		select {
		case <-s.gcCloser.HasBeenClosed():
			return
		case req := <-s.needTs: //处理时间戳获取 NOTE:2025060502
		slurpLoop:
			for {
				reqs = append(reqs, req) //将取出来的时间戳获取请求放到请求列中
				select {
				case req = <-s.needTs:
				default:
					break slurpLoop
				}
			}
		}

		// Generate the request.生成请求
		num := &pb.Num{}
		for _, r := range reqs {
			if r.readOnly {
				num.ReadOnly = true
			} else {
				num.Val++
			}
		}

		// Execute the request with infinite retries.
		// 无限次重试执行请求。
	retry:
		if s.gcCloser.Ctx().Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(s.gcCloser.Ctx(), 10*time.Second)
		ts, err := Timestamps(ctx, num) // NOTE:核心操作，向zero请求时间戳！！！
		cancel()
		if err != nil {
			glog.Warningf("Error while retrieving timestamps: %v with delay: %v."+
				" Will retry...\n", err, delay)
			time.Sleep(delay)
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
			goto retry
		}
		var offset uint64
		for _, req := range reqs { //遍历塞时间戳
			if req.readOnly { // 如果请求的是只读的时间戳
				req.ch <- ts.ReadOnly
			} else {
				req.ch <- ts.StartId + offset
				offset++
			}
		}
		x.AssertTrue(ts.StartId == 0 || ts.StartId+offset-1 == ts.EndId)
	}
}

type tsReq struct {
	readOnly bool
	// A one-shot chan which we can send a txn timestamp upon.
	// 一个一次性的chan，我们可以发送一个txn时间戳，这个ch里面就是存的时间戳，这里面有数据了，那就代表请求到了。
	ch chan uint64 
}
