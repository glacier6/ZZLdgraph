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
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof" // http profiler
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.opencensus.io/plugin/ocgrpc"
	otrace "go.opencensus.io/trace"
	"go.opencensus.io/zpages"
	"golang.org/x/net/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // grpc compression
	"google.golang.org/grpc/health"
	hapi "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v240/protos/api"
	_ "github.com/dgraph-io/gqlparser/v2/validator/rules" // make gql validator init() all rules
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/hypermodeinc/dgraph/v24/edgraph"
	"github.com/hypermodeinc/dgraph/v24/ee"
	"github.com/hypermodeinc/dgraph/v24/ee/audit"
	"github.com/hypermodeinc/dgraph/v24/ee/enc"
	"github.com/hypermodeinc/dgraph/v24/graphql/admin"
	"github.com/hypermodeinc/dgraph/v24/posting"
	"github.com/hypermodeinc/dgraph/v24/schema"
	"github.com/hypermodeinc/dgraph/v24/tok"
	"github.com/hypermodeinc/dgraph/v24/worker"
	"github.com/hypermodeinc/dgraph/v24/x"
)

var (
	bindall bool

	// used for computing uptime
	startTime = time.Now()

	// Alpha is the sub-command invoked when running "dgraph alpha".
	// Alpha是运行“dgraph-Alpha”时调用的子命令。
	Alpha x.SubCommand

	// need this here to refer it in admin_backup.go
	adminServer admin.IServeGraphQL
	initDone    uint32
)

func init() {
	// alpha节点的初始化
	Alpha.Cmd = &cobra.Command{
		Use:   "alpha",
		Short: "Run Dgraph Alpha database server",
		Long: `
A Dgraph Alpha instance stores the data. Each Dgraph Alpha is responsible for
storing and serving one data group. If multiple Alphas serve the same group,
they form a Raft group and provide synchronous replication.
`,
// Dgraph Alpha实例存储数据。每个Dgraph Alpha负责
// 存储和服务一个数据组。如果多个阿尔法服务于同一组，
// 它们形成一个Raft组并提供同步复制
		Run: func(cmd *cobra.Command, args []string) {
			defer x.StartProfile(Alpha.Conf).Stop()
			run() //NOTE:核心操作，如进行一些服务请求处理的绑定，一切处理请求的开端  NOTE:4100
		},
		Annotations: map[string]string{"group": "core"},
	}
	Alpha.EnvPrefix = "DGRAPH_ALPHA"
	Alpha.Cmd.SetHelpTemplate(x.NonRootTemplate)

	// If you change any of the flags below, you must also update run() to call Alpha.Conf.Get
	// with the flag name so that the values are picked up by Cobra/Viper's various config inputs
	// (e.g, config file, env vars, cli flags, etc.)
	// 如果你更改了下面的任何标志，你还必须更新run（）以调用Alpha。Conf.使用标志名称获取，以便Cobra/Viper的各种配置输入（例如配置文件、环境变量、cli标志等）获取值
	flag := Alpha.Cmd.Flags() // NOTE:核心论点，创建命令行解析对象，且全部存储在Alpha.Cmd.flags内
	// flag.String(), Bool(), Int()等函数注册flag
	// flag.Parse()函数解析在命令行使用了的flag。
	// 后面有关flag的都不用看了，只需知道flag是做CLI语法解析的就行

	// common ，填充ZERO与flag共有的一些解析语句
	x.FillCommonFlags(flag)
	// --tls SuperFlag
	x.RegisterServerTLSFlags(flag)
	// --encryption and --vault Superflag
	ee.RegisterAclAndEncFlags(flag)

	//下面是定义命令行的输入参数
	// String函数定义了三个参数依次为一个CLI参数的 name（如常用的help）、 默认值和用法的CLI参数。
	// 返回值是存储标志值的字符串变量的地址。
	// StringP类似于String，但接受一个可以在单个破折号后使用的简写字母。
	flag.StringP("postings", "p", "p", "Directory to store posting lists.") // 用于存储发布列表的目录
	flag.String("tmp", "t", "Directory to store temporary buffers.") // 用于存储临时缓冲区的目录

	flag.StringP("wal", "w", "w", "Directory to store raft write-ahead logs.") // 存储RAFT的WAL的目录地址
	flag.String("export", "export", "Folder in which to store exports.") // 用于存储导出的文件夹。
	flag.StringP("zero", "z", fmt.Sprintf("localhost:%d", x.PortZeroGrpc),
		"Comma separated list of Dgraph Zero addresses of the form IP_ADDRESS:PORT.")

	// Useful for running multiple servers on the same machine.
	flag.IntP("port_offset", "o", 0,
		"Value added to all listening port numbers. [Internal=7080, HTTP=8080, Grpc=9080]")

	// Custom plugins.
	flag.String("custom_tokenizers", "",
		"Comma separated list of tokenizer plugins for custom indices.")

	// By default Go GRPC traces all requests.
	grpc.EnableTracing = false

	flag.String("badger", worker.BadgerDefaults, z.NewSuperFlagHelp(worker.BadgerDefaults).
		Head("Badger options (Refer to badger documentation for all possible options)").
		Flag("compression",
			`[none, zstd:level, snappy] Specifies the compression algorithm and
			compression level (if applicable) for the postings directory."none" would disable
			compression, while "zstd:1" would set zstd compression at level 1.`).
		Flag("numgoroutines",
			"The number of goroutines to use in badger.Stream.").
		String())

	// Cache flags.
	flag.String("cache", worker.CacheDefaults, z.NewSuperFlagHelp(worker.CacheDefaults).
		Head("Cache options").
		Flag("size-mb",
			"Total size of cache (in MB) to be used in Dgraph.").
		Flag("percentage",
			"Cache percentages summing up to 100 for various caches (FORMAT: PostingListCache,"+
				"PstoreBlockCache,PstoreIndexCache)").
		Flag("delete-on-updates",
			"When set as true, we would delete the key from the cache once it's updated. If it's not "+
				"we would update the value inside the cache. If the cache gets too full, it starts"+
				" to get slow. So if your usecase has a lot of heavy mutations, this should be set"+
				" as true. If you are modifying same data again and again, this should be set as"+
				" false").
		String())

	flag.String("raft", worker.RaftDefaults, z.NewSuperFlagHelp(worker.RaftDefaults).
		Head("Raft options").
		Flag("idx",
			"Provides an optional Raft ID that this Alpha would use to join Raft groups.").
		Flag("group",
			"Provides an optional Raft Group ID that this Alpha would indicate to Zero to join.").
		Flag("learner",
			`Make this Alpha a "learner" node. In learner mode, this Alpha will not participate `+
				"in Raft elections. This can be used to achieve a read-only replica.").
		Flag("snapshot-after-entries",
			"Create a new Raft snapshot after N number of Raft entries. The lower this number, "+
				"the more frequent snapshot creation will be. Snapshots are created only if both "+
				"snapshot-after-duration and snapshot-after-entries threshold are crossed.").
		Flag("snapshot-after-duration",
			"Frequency at which we should create a new raft snapshots. Set "+
				"to 0 to disable duration based snapshot.").
		Flag("pending-proposals",
			"Number of pending mutation proposals. Useful for rate limiting.").
		String())

	flag.String("security", worker.SecurityDefaults, z.NewSuperFlagHelp(worker.SecurityDefaults).
		Head("Security options").
		Flag("token",
			"If set, all Admin requests to Dgraph will need to have this token. The token can be "+
				"passed as follows: for HTTP requests, in the X-Dgraph-AuthToken header. For Grpc, "+
				"in auth-token key in the context.").
		Flag("whitelist",
			"A comma separated list of IP addresses, IP ranges, CIDR blocks, or hostnames you wish "+
				"to whitelist for performing admin actions (i.e., --security "+
				`"whitelist=144.142.126.254,127.0.0.1:127.0.0.3,192.168.0.0/16,host.docker.`+
				`internal").`).
		String())

	flag.String("limit", worker.LimitDefaults, z.NewSuperFlagHelp(worker.LimitDefaults).
		Head("Limit options").
		Flag("query-edge",
			"The maximum number of edges that can be returned in a query. This applies to shortest "+
				"path and recursive queries.").
		Flag("normalize-node",
			"The maximum number of nodes that can be returned in a query that uses the normalize "+
				"directive.").
		Flag("mutations",
			"[allow, disallow, strict] The mutations mode to use.").
		Flag("mutations-nquad",
			"The maximum number of nquads that can be inserted in a mutation request.").
		Flag("disallow-drop",
			"Set disallow-drop to true to block drop-all and drop-data operation. It still"+
				" allows dropping attributes and types.").
		Flag("max-pending-queries",
			"Number of maximum pending queries before we reject them as too many requests.").
		Flag("query-timeout",
			"Maximum time after which a query execution will fail. If set to"+
				" 0, the timeout is infinite.").
		Flag("max-retries",
			"Commits to disk will give up after these number of retries to prevent locking the "+
				"worker in a failed state. Use -1 to retry infinitely.").
		Flag("txn-abort-after", "Abort any pending transactions older than this duration."+
			" The liveness of a transaction is determined by its last mutation.").
		Flag("shared-instance", "When set to true, it disables ACLs for non-galaxy users. "+
			"It expects the access JWT to be constructed outside dgraph for non-galaxy users as "+
			"login is denied to them. Additionally, this disables access to environment variables for minio, aws, etc.").
		Flag("type-filter-uid-limit", "TypeFilterUidLimit decides how many elements would be searched directly"+
			" vs searched via type index. If the number of elements are too low, then querying the"+
			" index might be slower. This would allow people to set their limit according to"+
			" their use case.").
		String())

	flag.String("graphql", worker.GraphQLDefaults, z.NewSuperFlagHelp(worker.GraphQLDefaults).
		Head("GraphQL options").
		Flag("introspection",
			"Enables GraphQL schema introspection.").
		Flag("debug",
			"Enables debug mode in GraphQL. This returns auth errors to clients, and we do not "+
				"recommend turning it on for production.").
		Flag("extensions",
			"Enables extensions in GraphQL response body.").
		Flag("poll-interval",
			"The polling interval for GraphQL subscription.").
		Flag("lambda-url",
			"The URL of a lambda server that implements custom GraphQL Javascript resolvers.").
		String())

	flag.String("cdc", worker.CDCDefaults, z.NewSuperFlagHelp(worker.CDCDefaults).
		Head("Change Data Capture options").
		Flag("file",
			"The path where audit logs will be stored.").
		Flag("kafka",
			"A comma separated list of Kafka hosts.").
		Flag("sasl-user",
			"The SASL username for Kafka.").
		Flag("sasl-password",
			"The SASL password for Kafka.").
		Flag("sasl-mechanism",
			"The SASL mechanism for Kafka (PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512)").
		Flag("ca-cert",
			"The path to CA cert file for TLS encryption.").
		Flag("client-cert",
			"The path to client cert file for TLS encryption.").
		Flag("client-key",
			"The path to client key file for TLS encryption.").
		String())

	flag.String("audit", worker.AuditDefaults, z.NewSuperFlagHelp(worker.AuditDefaults).
		Head("Audit options").
		Flag("output",
			`[stdout, /path/to/dir] This specifies where audit logs should be output to.
			"stdout" is for standard output. You can also specify the directory where audit logs
			will be saved. When stdout is specified as output other fields will be ignored.`).
		Flag("compress",
			"Enables the compression of old audit logs.").
		Flag("encrypt-file",
			"The path to the key file to be used for audit log encryption.").
		Flag("days",
			"The number of days audit logs will be preserved.").
		Flag("size",
			"The audit log max size in MB after which it will be rolled over.").
		String())

	flag.String("feature-flags", worker.FeatureFlagsDefaults, z.NewSuperFlagHelp(worker.FeatureFlagsDefaults).
		Head("Feature flags to enable various experimental features").
		Flag("normalize-compatibility-mode", "configure @normalize response formatting."+
			" 'v20': returns values with repeated key for fields with same alias (same as v20.11)."+
			" For more details, see https://github.com/hypermodeinc/dgraph/pull/7639").
		String())
}

func setupCustomTokenizers() {
	customTokenizers := Alpha.Conf.GetString("custom_tokenizers")
	if customTokenizers == "" {
		return
	}
	for _, soFile := range strings.Split(customTokenizers, ",") {
		tok.LoadCustomTokenizer(soFile)
	}
}

// Parses a comma-delimited list of IP addresses, IP ranges, CIDR blocks, or hostnames
// and returns a slice of []IPRange.
//
// e.g. "144.142.126.222:144.142.126.244,144.142.126.254,192.168.0.0/16,host.docker.internal"
func getIPsFromString(str string) ([]x.IPRange, error) {
	if str == "" {
		return []x.IPRange{}, nil
	}

	var ipRanges []x.IPRange
	rangeStrings := strings.Split(str, ",")

	for _, s := range rangeStrings {
		isIPv6 := strings.Contains(s, "::")
		tuple := strings.Split(s, ":")
		switch {
		case isIPv6 || len(tuple) == 1:
			if !strings.Contains(s, "/") {
				// string is hostname like host.docker.internal,
				// or IPv4 address like 144.124.126.254,
				// or IPv6 address like fd03:b188:0f3c:9ec4::babe:face
				ipAddr := net.ParseIP(s)
				if ipAddr != nil {
					ipRanges = append(ipRanges, x.IPRange{Lower: ipAddr, Upper: ipAddr})
				} else {
					ipAddrs, err := net.LookupIP(s)
					if err != nil {
						return nil, errors.Errorf("invalid IP address or hostname: %s", s)
					}

					for _, addr := range ipAddrs {
						ipRanges = append(ipRanges, x.IPRange{Lower: addr, Upper: addr})
					}
				}
			} else {
				// string is CIDR block like 192.168.0.0/16 or fd03:b188:0f3c:9ec4::/64
				rangeLo, network, err := net.ParseCIDR(s)
				if err != nil {
					return nil, errors.Errorf("invalid CIDR block: %s", s)
				}

				addrLen, maskLen := len(rangeLo), len(network.Mask)
				rangeHi := make(net.IP, len(rangeLo))
				copy(rangeHi, rangeLo)
				for i := 1; i <= maskLen; i++ {
					rangeHi[addrLen-i] |= ^network.Mask[maskLen-i]
				}

				ipRanges = append(ipRanges, x.IPRange{Lower: rangeLo, Upper: rangeHi})
			}
		case len(tuple) == 2:
			// string is range like a.b.c.d:w.x.y.z
			rangeLo := net.ParseIP(tuple[0])
			rangeHi := net.ParseIP(tuple[1])
			switch {
			case rangeLo == nil:
				return nil, errors.Errorf("invalid IP address: %s", tuple[0])
			case rangeHi == nil:
				return nil, errors.Errorf("invalid IP address: %s", tuple[1])
			case bytes.Compare(rangeLo, rangeHi) > 0:
				return nil, errors.Errorf("inverted IP address range: %s", s)
			}
			ipRanges = append(ipRanges, x.IPRange{Lower: rangeLo, Upper: rangeHi})
		default:
			return nil, errors.Errorf("invalid IP address range: %s", s)
		}
	}

	return ipRanges, nil
}

func httpPort() int {
	return x.Config.PortOffset + x.PortHTTP
}

func grpcPort() int {
	return x.Config.PortOffset + x.PortGrpc
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	x.AddCorsHeaders(w)
	var err error

	if _, ok := r.URL.Query()["all"]; ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		ctx := x.AttachAccessJwt(context.Background(), r)
		var resp *api.Response
		if resp, err = (&edgraph.Server{}).Health(ctx, true); err != nil {
			x.SetStatus(w, x.Error, err.Error())
			return
		}
		if resp == nil {
			x.SetStatus(w, x.ErrorNoData, "No health information available.")
			return
		}
		_, _ = w.Write(resp.Json)
		return
	}

	_, ok := r.URL.Query()["live"]
	if !ok {
		if err := x.HealthCheck(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, err = w.Write([]byte(err.Error()))
			if err != nil {
				glog.V(2).Infof("Error while writing health check response: %v", err)
			}
			return
		}
	}

	var resp *api.Response
	if resp, err = (&edgraph.Server{}).Health(context.Background(), false); err != nil {
		x.SetStatus(w, x.Error, err.Error())
		return
	}
	if resp == nil {
		x.SetStatus(w, x.ErrorNoData, "No health information available.")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp.Json)
}

func stateHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	x.AddCorsHeaders(w)
	w.Header().Set("Content-Type", "application/json")

	ctx := context.Background()
	ctx = x.AttachAccessJwt(ctx, r)

	var aResp *api.Response
	if aResp, err = (&edgraph.Server{}).State(ctx); err != nil {
		x.SetStatus(w, x.Error, err.Error())
		return
	}
	if aResp == nil {
		x.SetStatus(w, x.ErrorNoData, "No state information available.")
		return
	}

	if _, err = w.Write(aResp.Json); err != nil {
		x.SetStatus(w, x.Error, err.Error())
		return
	}
}

// storeStatsHandler outputs some basic stats for data store.
func storeStatsHandler(w http.ResponseWriter, r *http.Request) {
	x.AddCorsHeaders(w)
	w.Header().Set("Content-Type", "text/html")
	x.Check2(w.Write([]byte("<pre>")))
	x.Check2(w.Write([]byte(worker.StoreStats())))
	x.Check2(w.Write([]byte("</pre>")))
}

func setupListener(addr string, port int) (net.Listener, error) {
	return net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
}

func serveGRPC(l net.Listener, tlsCfg *tls.Config, closer *z.Closer) {
	defer closer.Done()

	x.RegisterExporters(Alpha.Conf, "dgraph.alpha")

	opt := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(x.GrpcMaxSize),
		grpc.MaxSendMsgSize(x.GrpcMaxSize),
		grpc.MaxConcurrentStreams(1000),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
		grpc.UnaryInterceptor(audit.AuditRequestGRPC),
	}
	if tlsCfg != nil {
		tlsCfg.NextProtos = []string{"h2"}
		opt = append(opt, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}

	s := grpc.NewServer(opt...)
	api.RegisterDgraphServer(s, &edgraph.Server{})
	hapi.RegisterHealthServer(s, health.NewServer())
	worker.RegisterZeroProxyServer(s)

	err := s.Serve(l)
	glog.Errorf("GRPC listener canceled: %v\n", err)
	s.Stop()
}

func setupServer(closer *z.Closer) {
	go worker.RunServer(bindall) // NOTE:核心操作，worker开始监听其他worker或raft方面的通信请求

	laddr := "localhost"
	if bindall {
		laddr = "0.0.0.0"
	}

	tlsCfg, err := x.LoadServerTLSConfig(Alpha.Conf) //加载安全协议tls的配置
	if err != nil {
		log.Fatalf("Failed to setup TLS: %v\n", err)
	}

	//下面是定义两个监听器（当前只建立了监听器net.Listener，没有server，server在下面的那两个协程里面 NOTE:05280），监听TCP连接，setupListener第一个参数是地址，第二个是监听的端口号
	httpListener, err := setupListener(laddr, httpPort())
	// http.Handle("/test", myHandler{})
	// http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}

	grpcListener, err := setupListener(laddr, grpcPort())
	if err != nil {
		log.Fatal(err)
	}

	// ServeMux 本质上是一个 HTTP 请求路由器（或者叫多路复用器，Multiplexor）。它把收到的请求与一组预先定义的 URL 路径列表做对比，然后在匹配到路径的时候调用关联的处理器（Handler）。
	baseMux := http.NewServeMux()
	// http.Handle 用于将一个实现了 http.Handler 接口的对象注册到指定的 URL 路径前缀上，当有匹配该路径前缀的 HTTP 请求到来时，会调用该对象的 ServeHTTP 方法进行处理。
	http.Handle("/", audit.AuditRequestHttp(baseMux))
	
	//NOTE:核心操作，以下均是，将各个处理函数绑定到多路复用器上对应请求路径上
	baseMux.HandleFunc("/query", queryHandler)  //查询
	baseMux.HandleFunc("/query/", queryHandler)
	baseMux.HandleFunc("/mutate", mutationHandler) //突变 TODO:待看突变，结合那个事务解读的一起看
	baseMux.HandleFunc("/mutate/", mutationHandler)
	baseMux.HandleFunc("/commit", commitHandler)
	baseMux.HandleFunc("/alter", alterHandler)
	baseMux.HandleFunc("/health", healthCheck)
	baseMux.HandleFunc("/state", stateHandler)
	baseMux.HandleFunc("/debug/jemalloc", x.JemallocHandler)
	zpages.Handle(baseMux, "/debug/z")

	// TODO: Figure out what this is for?
	http.HandleFunc("/debug/store", storeStatsHandler)

	introspection := x.Config.GraphQL.GetBool("introspection")

	// Global Epoch is a lockless synchronization mechanism for graphql service.
	// It's is just an atomic counter used by the graphql subscription to update its state.
	// It's is used to detect the schema changes and server exit.
	// It is also reported by /probe/graphql endpoint as the schemaUpdateCounter.
	// Global Epoch是graphql服务的无锁同步机制。
	// 它只是graphql订阅用来更新其状态的原子计数器。
	// 它用于检测schema更改和服务器退出。
	// 它也被/probe/graphql端点报告为schemaUpdateCounter。

	// Implementation for schema change:
	// The global epoch is incremented when there is a schema change.
	// Polling goroutine acquires the current epoch count as a local epoch.
	// The local epoch count is checked against the global epoch,
	// If there is change then we terminate the subscription.
	// schema更改的实现：
	// 当schema发生变化时，GlobalEpoch会递增。
	// 轮询goroutine获取当前epoch计数作为本地epoch。
	// 将局部epoch计数与全局历元进行核对，
	// 如果有变化，我们将终止订阅。

	// Implementation for server exit:
	// The global epoch is set to maxUint64 while exiting the server.
	// By using this information polling goroutine terminates the subscription.
	// 服务器退出实现：
	// 退出服务器时，GlobalEpoch设置为maxUint64。
	// 通过使用此信息轮询，goroutine终止订阅。
	globalEpoch := make(map[uint64]*uint64)
	e := new(uint64)
	atomic.StoreUint64(e, 0)
	globalEpoch[x.GalaxyNamespace] = e
	var mainServer admin.IServeGraphQL
	var gqlHealthStore *admin.GraphQLHealthStore
	// Do not use := notation here because adminServer is a global variable.
	mainServer, adminServer, gqlHealthStore = admin.NewServers(introspection,
		globalEpoch, closer)
	baseMux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		namespace := x.ExtractNamespaceHTTP(r)
		r.Header.Set("resolver", strconv.FormatUint(namespace, 10))
		if err := admin.LazyLoadSchema(namespace); err != nil {
			admin.WriteErrorResponse(w, r, err)
			return
		}
		mainServer.HTTPHandler().ServeHTTP(w, r)
	})

	baseMux.Handle("/probe/graphql", graphqlProbeHandler(gqlHealthStore, globalEpoch))

	baseMux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("resolver", "0")
		// We don't need to load the schema for all the admin operations.
		// Only a few like getUser, queryGroup require this. So, this can be optimized.
		if err := admin.LazyLoadSchema(x.ExtractNamespaceHTTP(r)); err != nil {
			admin.WriteErrorResponse(w, r, err)
			return
		}
		allowedMethodsHandler(allowedMethods{
			http.MethodGet:     true,
			http.MethodPost:    true,
			http.MethodOptions: true,
		}, adminAuthHandler(adminServer.HTTPHandler())).ServeHTTP(w, r)
	})
	baseMux.Handle("/admin/", getAdminMux())

	addr := fmt.Sprintf("%s:%d", laddr, httpPort())
	glog.Infof("Bringing up GraphQL HTTP API at %s/graphql", addr)
	glog.Infof("Bringing up GraphQL HTTP admin API at %s/admin", addr)

	baseMux.Handle("/", http.HandlerFunc(homeHandler))
	baseMux.Handle("/ui/keywords", http.HandlerFunc(keywordHandler))

	// Initialize the servers.
	// 初始化服务器，开启两个协程去监听http与GRPC （NOTE:05280）
	x.ServerCloser.AddRunning(3)
	go serveGRPC(grpcListener, tlsCfg, x.ServerCloser) //监听GRPC请求
	go x.StartListenHttpAndHttps(httpListener, tlsCfg, x.ServerCloser)//监听http请求，貌似上面注册的链接会因为HTTP包内部的操作默认放到http内部上？

	go func() {
		defer x.ServerCloser.Done()

		<-x.ServerCloser.HasBeenClosed()
		// TODO - Verify why do we do this and does it have to be done for all namespaces.
		e = globalEpoch[x.GalaxyNamespace]
		atomic.StoreUint64(e, math.MaxUint64)

		// Stops grpc/http servers; Already accepted connections are not closed.
		if err := grpcListener.Close(); err != nil {
			glog.Warningf("Error while closing gRPC listener: %s", err)
		}
		if err := httpListener.Close(); err != nil {
			glog.Warningf("Error while closing HTTP listener: %s", err)
		}
	}()

	glog.Infoln("gRPC server started.  Listening on port", grpcPort())
	glog.Infoln("HTTP server started.  Listening on port", httpPort())

	atomic.AddUint32(&initDone, 1)
	// Audit needs groupId and nodeId to initialize audit files
	// Therefore we wait for the cluster initialization to be done.
	for {
		if x.HealthCheck() == nil {
			// Audit is enterprise feature.
			x.Check(audit.InitAuditorIfNecessary(worker.Config.Audit, worker.EnterpriseEnabled))
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	x.ServerCloser.Wait()
}

// 运行run时就已经有CLI的参数了，在本函数中主要是对各种配置的获取、解析以及设置
func run() {
	var err error

	// SuperFlag是超级标志（CLI中用），其内会包含很多子标志（即各类配置），如telemetry，badger，cache等等都是超级标志
	// 超级标志的一般语法如下： --<super-flag-name> option-a=value; option-b=value
	// 而telemetry控制的是将监控数据发送给Dgraph开发人员。
	telemetry := z.NewSuperFlag(Alpha.Conf.GetString("telemetry")).MergeAndCheckDefault(  // 而NewSuperFlag就是把CLI的超级标志字符串解析成可以使用的对象
		x.TelemetryDefaults)
	// Sentry是一个开源的应用性能监控（APM）和错误追踪平台
	if telemetry.GetBool("sentry") { //如果使用性能监控
		x.InitSentry(enc.EeBuild)
		defer x.FlushSentry()
		x.ConfigureSentryScope("alpha")
		x.WrapPanics()
		x.SentryOptOutNote()
	}

	bindall = Alpha.Conf.GetBool("bindall")
	cache := z.NewSuperFlag(Alpha.Conf.GetString("cache")).MergeAndCheckDefault(
		worker.CacheDefaults)  //得到cache的配置项
	totalCache := cache.GetInt64("size-mb") // 得到配置分得的总cache大小
	x.AssertTruef(totalCache >= 0, "ERROR: Cache size must be non-negative")

	cachePercentage := cache.GetString("percentage") // 得到缓存占比配置的字符串
	deleteOnUpdates := cache.GetBool("delete-on-updates")
	cachePercent, err := x.GetCachePercentages(cachePercentage, 3) // 解析出来得到百分比切片
	x.Check(err)
	postingListCacheSize := (cachePercent[0] * (totalCache << 20)) / 100 // 得到postingList的cache大小
	pstoreBlockCacheSize := (cachePercent[1] * (totalCache << 20)) / 100 // 得到Badger块缓存的cache大小
	pstoreIndexCacheSize := (cachePercent[2] * (totalCache << 20)) / 100 // 得到Badger索引缓存的cache大小

	cacheOpts := fmt.Sprintf("blockcachesize=%d; indexcachesize=%d; ",
		pstoreBlockCacheSize, pstoreIndexCacheSize)
	bopts := badger.DefaultOptions("").FromSuperFlag(worker.BadgerDefaults + cacheOpts).
		FromSuperFlag(Alpha.Conf.GetString("badger")) // 得到badger的总配置项
	security := z.NewSuperFlag(Alpha.Conf.GetString("security")).MergeAndCheckDefault(
		worker.SecurityDefaults) // 解析CLI输入的超级标志字符串中解析得到有关安全的超级标志
	conf := audit.GetAuditConf(Alpha.Conf.GetString("audit"))

	x.Config.Limit = z.NewSuperFlag(Alpha.Conf.GetString("limit")).MergeAndCheckDefault(
		worker.LimitDefaults)  //设置各种最大值，比如 允许并发处理的最大请求数量 ，查询中可以返回的最大边数等等

	// 下面是填充worker的配置项
	opts := worker.Options{ 
		PostingDir:      Alpha.Conf.GetString("postings"),
		WALDir:          Alpha.Conf.GetString("wal"),
		CacheMb:         totalCache,
		CachePercentage: cachePercentage,
		DeleteOnUpdates: deleteOnUpdates,

		MutationsMode:      worker.AllowMutations,
		AuthToken:          security.GetString("token"),
		Audit:              conf,
		ChangeDataConf:     Alpha.Conf.GetString("cdc"),
		TypeFilterUidLimit: x.Config.Limit.GetUint64("type-filter-uid-limit"),
	}

	keys, err := ee.GetKeys(Alpha.Conf)
	x.Check(err)

	// 下面是有关访问控制列表ACL的，也暂时先不看
	if keys.AclSecretKey != nil {
		opts.AclJwtAlg = keys.AclJwtAlg
		opts.AclSecretKey = keys.AclSecretKey
		opts.AclSecretKeyBytes = keys.AclSecretKeyBytes
		opts.AccessJwtTtl = keys.AclAccessTtl
		opts.RefreshJwtTtl = keys.AclRefreshTtl
		glog.Info("ACL secret key loaded successfully.")
	}

	//下面是依照限制的设置，来设置WORKER的工作模式
	abortDur := x.Config.Limit.GetDuration("txn-abort-after")
	switch strings.ToLower(x.Config.Limit.GetString("mutations")) {
	case "allow":
		opts.MutationsMode = worker.AllowMutations // 普通的允许变更模式
	case "disallow":
		opts.MutationsMode = worker.DisallowMutations // 不允许变更模式
	case "strict":
		opts.MutationsMode = worker.StrictMutations // 严格变更模式
	default:
		glog.Error(`--limit "mutations=<mode>;" must be one of allow, disallow, or strict`)
		os.Exit(1)
	}

	worker.SetConfiguration(&opts) //给worker设置配置项

	ips, err := getIPsFromString(security.GetString("whitelist")) //得到白名单的IP地址切片
	x.Check(err)
	
	// 下面是使用tls确保集群端口连接安全的，也不用看
	tlsClientConf, err := x.LoadClientTLSConfigForInternalPort(Alpha.Conf)
	x.Check(err)
	tlsServerConf, err := x.LoadServerTLSConfigForInternalPort(Alpha.Conf)
	x.Check(err)

	raft := z.NewSuperFlag(Alpha.Conf.GetString("raft")).MergeAndCheckDefault(worker.RaftDefaults) //构造raft的超级标志
	x.WorkerConfig = x.WorkerOptions{  // 集成上面得到的所有配置项，生成worker用的全局配置对象
		TmpDir:              Alpha.Conf.GetString("tmp"),
		ExportPath:          Alpha.Conf.GetString("export"),
		ZeroAddr:            strings.Split(Alpha.Conf.GetString("zero"), ","),
		Raft:                raft,
		WhiteListedIPRanges: ips,
		StrictMutations:     opts.MutationsMode == worker.StrictMutations,
		AclEnabled:          keys.AclSecretKey != nil,
		AbortOlderThan:      abortDur,
		StartTime:           startTime,
		Security:            security,
		TLSClientConfig:     tlsClientConf,
		TLSServerConfig:     tlsServerConf,
		AclJwtAlg:           keys.AclJwtAlg,
		AclPublicKey:        keys.AclPublicKey,
		Audit:               opts.Audit != nil,
		Badger:              bopts,
	}
	x.WorkerConfig.Parse(Alpha.Conf)

	if telemetry.GetBool("reports") {
		go edgraph.PeriodicallyPostTelemetry()
	}

	// Set the directory for temporary buffers.
	// 设置临时缓冲区的目录。
	z.SetTmpDir(x.WorkerConfig.TmpDir)

	x.WorkerConfig.EncryptionKey = keys.EncKey

	// 下面是在x.Config对象内设置全局配置（很多配置基本都来自Alpha对象，该对象内存着在CLI中运行时配置的各个参数）
	setupCustomTokenizers()
	x.Config.PortOffset = Alpha.Conf.GetInt("port_offset")  //这行是得到CLI中配置的值（或者默认值）
	x.Config.LimitMutationsNquad = int(x.Config.Limit.GetInt64("mutations-nquad"))
	x.Config.LimitQueryEdge = x.Config.Limit.GetUint64("query-edge")
	x.Config.BlockClusterWideDrop = x.Config.Limit.GetBool("disallow-drop")
	x.Config.LimitNormalizeNode = int(x.Config.Limit.GetInt64("normalize-node"))
	x.Config.QueryTimeout = x.Config.Limit.GetDuration("query-timeout")
	x.Config.MaxRetries = x.Config.Limit.GetInt64("max-retries")
	x.Config.SharedInstance = x.Config.Limit.GetBool("shared-instance")

	x.Config.GraphQL = z.NewSuperFlag(Alpha.Conf.GetString("graphql")).MergeAndCheckDefault(
		worker.GraphQLDefaults)
	x.Config.GraphQLDebug = x.Config.GraphQL.GetBool("debug")
	if x.Config.GraphQL.GetString("lambda-url") != "" {
		graphqlLambdaUrl, err := url.Parse(x.Config.GraphQL.GetString("lambda-url"))
		if err != nil {
			glog.Errorf("unable to parse --graphql lambda-url: %v", err)
			return
		}
		if !graphqlLambdaUrl.IsAbs() {
			glog.Errorf("expecting --graphql lambda-url to be an absolute URL, got: %s",
				graphqlLambdaUrl.String())
			return
		}
	}
	edgraph.Init() // 里面只做了 设置最大等待查询数 这一个操作

	// feature flags
	featureFlagsConf := z.NewSuperFlag(Alpha.Conf.GetString("feature-flags")).MergeAndCheckDefault(
		worker.FeatureFlagsDefaults)
	x.Config.NormalizeCompatibilityMode = featureFlagsConf.GetString("normalize-compatibility-mode")

	x.PrintVersion()
	glog.Infof("x.Config: %+v", x.Config)
	glog.Infof("x.WorkerConfig: %+v", x.WorkerConfig)
	glog.Infof("worker.Config: %+v", worker.Config)

	worker.InitServerState() // 初始化worker的状态
	worker.InitTasks() // 初始化worker的全局任务列表对象

	if Alpha.Conf.GetBool("expose_trace") {
		// TODO: Remove this once we get rid of event logs.
		trace.AuthRequest = func(req *http.Request) (any, sensitive bool) {
			return true, true
		}
	}
	otrace.ApplyConfig(otrace.Config{
		DefaultSampler:             otrace.ProbabilitySampler(x.WorkerConfig.Trace.GetFloat64("ratio")),
		MaxAnnotationEventsPerSpan: 256,
	})

	// Posting will initialize index which requires schema. Hence, initialize
	// schema before calling posting.Init().
	// Posting的初始化需要schema。因此，在调用posting之前初始化schema。Init（）。
	schema.Init(worker.State.Pstore) // 按照在BadgerDB中已有数据初始化schema
	posting.Init(worker.State.Pstore, postingListCacheSize, deleteOnUpdates) // 初始化PostingList
	defer posting.Cleanup()
	worker.Init(worker.State.Pstore) //初始化worker，里面主要是对worker的gRPC进行初始化

	// setup shutdown os signal handler
	// 设置关闭操作系统信号处理程序
	sdCh := make(chan os.Signal, 3)

	// 下面这一块是开启监听关闭的协程（如在命令行按ctrl+c ？）
	defer func() {
		signal.Stop(sdCh)
		close(sdCh)
	}()
	// sigint : Ctrl-C, sigterm : kill command.
	signal.Notify(sdCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		var numShutDownSig int
		for range sdCh {
			closer := x.ServerCloser
			select {
			case <-closer.HasBeenClosed():
			default:
				closer.Signal()
			}
			numShutDownSig++
			glog.Infoln("Caught Ctrl-C. Terminating now (this may take a few seconds)...")

			switch {
			case atomic.LoadUint32(&initDone) < 2:
				// Forcefully kill alpha if we haven't finish server initialization.
				glog.Infoln("Stopped before initialization completed")
				os.Exit(1)
			case numShutDownSig == 3:
				glog.Infoln("Signaled thrice. Aborting!")
				os.Exit(1)
			}
		}
	}()

	updaters := z.NewCloser(2)
	go func() {
		worker.StartRaftNodes(worker.State.WALstore, bindall) // NOTE:核心操作，使当前的worker进入Raft的
		atomic.AddUint32(&initDone, 1)

		// initialization of the admin account can only be done after raft nodes are running
		// and health check passes
		//只有在raft节点运行并且健康检查通过后，才能初始化管理员帐户
		edgraph.InitializeAcl(updaters)
		edgraph.RefreshACLs(updaters.Ctx())
		edgraph.SubscribeForAclUpdates(updaters)
	}()

	// Graphql subscribes to alpha to get schema updates. We need to close that before we
	// close alpha. This closer is for closing and waiting that subscription.
	// Graphql订阅alpha以获取模式更新。我们需要在关闭alpha之前关闭它。此封隔器用于关闭并等待订阅。
	adminCloser := z.NewCloser(1)

	setupServer(adminCloser) //NOTE:核心操作，绑定服务处理函数
	glog.Infoln("GRPC and HTTP stopped.")

	//下面都是有关各种关闭的 

	// This might not close until group is given the signal to close. So, only signal here,
	// wait for it after group is closed.
	// 在组收到关闭信号之前，这可能不会关闭。所以，这里只有信号，请在小组关闭后等待
	updaters.Signal() 

	worker.BlockingStop()
	glog.Infoln("worker stopped.")

	adminCloser.SignalAndWait()
	glog.Infoln("adminCloser closed.")

	audit.Close()

	worker.State.Dispose()
	x.RemoveCidFile()
	glog.Info("worker.State disposed.")

	updaters.Wait()
	glog.Infoln("updaters closed.")

	glog.Infoln("Server shutdown. Bye!")
}
