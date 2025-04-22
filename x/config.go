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

package x

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/spf13/viper"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/v2/z"
)

// Options stores the options for this package.
type Options struct {
	// PortOffset will be used to determine the ports to use (port = default port + offset).
	PortOffset int
	// Limit options:
	//
	// query-edge uint64 - maximum number of edges that can be returned in a query
	// normalize-node int - maximum number of nodes that can be returned in a query that uses the
	//                      normalize directive
	// mutations-nquad int - maximum number of nquads that can be inserted in a mutation request
	// BlockDropAll bool - if set to true, the drop all operation will be rejected by the server.
	// query-timeout duration - Maximum time after which a query execution will fail.
	// max-retries int64 - maximum number of retries made by dgraph to commit a transaction to disk.
	// shared-instance bool - if set to true, ACLs will be disabled for non-galaxy users.
	Limit                *z.SuperFlag
	LimitMutationsNquad  int
	LimitQueryEdge       uint64
	BlockClusterWideDrop bool
	LimitNormalizeNode   int
	QueryTimeout         time.Duration
	MaxRetries           int64
	SharedInstance       bool

	// GraphQL options:
	//
	// extensions bool - Will be set to see extensions in GraphQL results
	// debug bool - Will enable debug mode in GraphQL.
	// lambda-url string - Stores the URL of lambda functions for custom GraphQL resolvers
	// 			The configured lambda-url can have a parameter `$ns`,
	//			which should be replaced with the correct namespace value at runtime.
	// 	===========================================================================================
	// 	|                lambda-url                | $ns |           namespacedLambdaUrl          |
	// 	|==========================================|=====|========================================|
	// 	| http://localhost:8686/graphql-worker/$ns |  1  | http://localhost:8686/graphql-worker/1 |
	// 	| http://localhost:8686/graphql-worker     |  1  | http://localhost:8686/graphql-worker   |
	// 	|=========================================================================================|
	//
	// poll-interval duration - The polling interval for graphql subscription.
	GraphQL      *z.SuperFlag
	GraphQLDebug bool

	// feature flags
	NormalizeCompatibilityMode string
}

// Config stores the global instance of this package's options.
// Config存储此包配置的全局实例。
var Config Options

// IPRange represents an IP range.
type IPRange struct {
	Lower, Upper net.IP
}

// WorkerOptions stores the options for the worker package. It's declared here
// since it's used by multiple packages.
type WorkerOptions struct {

	// TmpDir is a directory to store temporary buffers.
	// TmpDir是一个用于存储临时缓冲区的目录。
	TmpDir string

	// ExportPath indicates the folder to which exported data will be saved.
	// ExportPath表示导出数据将保存到的文件夹。
	ExportPath string

	// Trace options:
	//
	// ratio float64 - the ratio of queries to trace (must be between 0 and 1)
	// jaeger string - URL of Jaeger to send OpenCensus traces
	// datadog string - URL of Datadog to send OpenCensus traces
	//跟踪选项：
	//ratio float64-查询与跟踪的比率（必须介于0和1之间）
	//jaeger string-发送OpenCensus跟踪的jaeger的URL
	//datadog字符串-发送OpenCensus跟踪的datadog的URL
	Trace *z.SuperFlag

	// MyAddr stores the address and port for this alpha.
	//MyAddr存储此alpha的地址和端口。
	MyAddr string

	// ZeroAddr stores the list of address:port for the zero instances associated with this alpha.
	// Alpha would communicate via only one zero address from the list. All
	// the other addresses serve as fallback.
	//ZeroAddr存储与此alpha关联的zero个实例的地址：端口列表。
	//alpha只能通过列表中的一个zero地址进行通信。所有其他地址都作为后备。
	ZeroAddr []string

	// TLS client config which will be used to connect with zero and alpha internally
	// TLS客户端配置，将用于在内部与zero和alpha连接，注意TLS是一个传输层安全协议，其前身是SSL
	TLSClientConfig *tls.Config

	// TLS server config which will be used to initiate server internal port
	// 用于启动服务器内部端口的TLS服务器配置
	TLSServerConfig *tls.Config

	// Raft stores options related to Raft.
	// Raft对象存储与Raft相关的配置。
	Raft *z.SuperFlag

	// Badger stores the badger options.
	Badger badger.Options

	// WhiteListedIPRanges is a list of IP ranges from which requests will be allowed.
	// IP地址白名单
	WhiteListedIPRanges []IPRange

	// StrictMutations will cause mutations to unknown predicates to fail if set to true.
	// 设置严格模式，未匹配已有谓词的突变将会被拒绝
	StrictMutations bool

	// AclEnabled indicates whether the enterprise ACL feature is turned on.
	// AclEnabled表示企业ACL功能是否已打开。
	AclEnabled bool

	// AclJwtAlg stores the JWT signing algorithm.
	// AclJwtAlg存储JWT签名算法。
	AclJwtAlg jwt.SigningMethod

	// AclPublicKey stores the public key used to verify JSON Web Tokens (JWT).
	// It could be a either a RSA or ECDSA PublicKey or HMAC symmetric key.
	// depending upon the JWT signing algorithm. Note that for symmetric algorithms,
	// this will contain the same key as the private key, needs to be used carefully.
	//AclPublicKey存储用于验证JSON Web令牌（JWT）的公钥。
	//它可以是RSA或ECDSA公钥或HMAC对称密钥。
	//这取决于JWT签名算法。注意，对于对称算法，
	//这将包含与私钥相同的密钥，需要谨慎使用。
	AclPublicKey interface{}

	// AbortOlderThan tells Dgraph to discard transactions that are older than this duration.
	// 持续时间超过AbortOlderThan的事务将会被舍弃。
	AbortOlderThan time.Duration

	// ProposedGroupId will be used if there's a file in the p directory called group_id with the proposed group ID for this server.
	// AbortOlderThan告诉Dgraph放弃早于此duratioProposedGroupId的事务，如果p目录中有一个名为group_id的文件具有此服务器的建议组id，则将使用该组id。
	ProposedGroupId uint32

	// StartTime is the start time of the alpha
	// StartTime存储alpha的开始时间
	StartTime time.Time

	// Security options:
	//
	// whitelist string - comma separated IP addresses
	// token string - if set, all Admin requests to Dgraph will have this token.
	// 安全选项：
	// 白名单字符串-逗号分隔的IP地址
	// 令牌字符串-如果设置，则所有对Dgraph的管理员请求都将具有此令牌。
	Security *z.SuperFlag

	// EncryptionKey is the key used for encryption at rest, backups, exports. Enterprise only feature.
	// EncryptionKey是用于静态加密、备份和导出的密钥。仅限企业功能。
	EncryptionKey Sensitive

	// LogDQLRequest indicates whether alpha should log all query/mutation requests coming to it.
	// Ideally LogDQLRequest should be a bool value. But we are reading it using atomics across
	// queries hence it has been kept as int32. LogDQLRequest value 1 enables logging of requests
	// coming to alphas and 0 disables it.
	//LogDQLRequest指示alpha是否应该记录所有到达它的查询/变异请求。
	//理想情况下，LogDQLRequest应该是一个bool值。但是我们在整个查询中使用原子来读取它，因此它一直保持为int32。LogDQLRequest值1启用对到达alpha的请求的日志记录，0禁用它。
	LogDQLRequest int32

	// If true, we should call msync or fsync after every write to survive hard reboots.
	// 如果为真，我们应该在每次写入后调用msync或fsync，以在硬重启后幸存下来。
	HardSync bool

	// Audit contains the audit flags that enables the audit.
	// 是否启动审计
	Audit bool
}

// WorkerConfig stores the global instance of the worker package's options.
// WorkerConfig是一个存储worker包配置的全局实例。
var WorkerConfig WorkerOptions

func (w *WorkerOptions) Parse(conf *viper.Viper) {
	w.MyAddr = conf.GetString("my")
	w.Trace = z.NewSuperFlag(conf.GetString("trace")).MergeAndCheckDefault(TraceDefaults)

	survive := conf.GetString("survive")
	AssertTruef(survive == "process" || survive == "filesystem",
		"Invalid survival mode: %s", survive)
	w.HardSync = survive == "filesystem"
}
