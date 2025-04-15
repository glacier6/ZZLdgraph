## ZZL写在最前：
### (1) Dgraph如何从源码构建运行环境，以方便DEBUG？Dgraph如何与修改的Badger联系起来？  
  首先VSCODE先下载好GO语言的工具包，详见https://blog.csdn.net/weixin_44387339/article/details/131633127  
  然后点击VSCODE左侧调试，分别本地运行alpha与zero即可（默认已经配置好端口号了），然后打断点，并在ratel里发送请求测试就行  
  当前ratel地址 [192.168.80.128](http://192.168.80.128:8000/?latest)  
  当前zero地址 http://192.168.80.128:8080  
  
###  (2) Dgraph的一些需要关注点  
  - 2.1 一些英语单词对照  
      - predicate 谓词  
      - Schema 模式  
      - facet 方面（在Dgraph中，常表示关系自身的属性，如权重，时间范围等）  
  - 2.2 谓词实际就是JSON格式中的左侧,有时也叫relationship（关系），或者边  
      右侧叫宾语（所以可以是字面值（literal values）也可以是另一个节点或节点数组（此时左侧谓词也叫关系relationship））  
      所属对象（节点）则可以叫为主语  
  - 2.3 有关Schema  
      -  Dgraph 维护了一个包含所有谓词名称及其类型和索引的 Dgraph 类型 schema 列表，而schema定义节点的结构体，提供数据的结构化定义。  
        具体来说的话，Schema会定义底层三元组中谓语指向的宾语是什么节点类型又或者是什么具体类型的字面量，而且Schema也会定义在哪个谓语中需要定义索引  
      -  GraphQL与DQL的区别看4.5  
        
  - 2.4 默认 Dgraph图中的关系（边）是有向的，不过可以用@hasInverse来告诉Dgraph如何处理双向关系，具体看官方文档 relationships 的内容  
  - 2.5 Dgraph支持自定义查询语句 详见文档的Custom DQL  
  - 2.6 Dgraph 按关系分片数据，因此一个关系的数据形成一个单独的分片，并存储在一个（组）服务器上，这种做法被称为“谓词分片”。
  - 2.7 聚合查询同学数据库的那个聚合函数，就是做统计的
  
### (3) Dgraph代码太庞大了，下面按分支来看  
  NOTE:4100  数据库绑定 各类请求的对应响应函数 的开端  
### (4) GraphQL与DQL  
  - 4.1 GraphQL与DQL均是在Dgraph后端图数据之上实现的查询与操作语言，但DQL是GraphQL的一个超集，其受GraphQL启发，但包含了更多特性（所以可以统统用DQL，看代码也先只看DQL的）  
  - 4.2 Dgraph的Graphql HTTP为: http://xxx.xxx.xxx.xxx:8080/graphql 在请求的body里面定义是query还是mutate  
    Dgraph的DQL的 HTTP是通过URI指定操作的，如: http://xxx.xxx.xxx.xxx:8080/query 用来查询  
        http://xxx.xxx.xxx.xxx:8080/mutate 用来执行 mutate操作  
  - 4.3 GraphQL 是一种强类型语言。与按端点组织的 REST 不同，GraphQL API 是按类型和字段组织的。类型系统用于定义模式，这是客户端和服务器之间的合同。GraphQL 使用类型来确保应用程序只请求可能的内容，并提供清晰且有用的错误信息。 
  - 4.4 DQL的查询语句与GraphQL的查询语句，模式定义等均不相同，但是其因为后端存的图数据是一个，可以GraphQL语句新增，DQL语句查询。  
  - 4.5 注意GraphQL（相当于ResetFul的一个东西）本身有一个模式的定义，而用GraphQL的时候，还会有一个将GraphQL的Schema转为Dgraph的Schema的过程，而GraphQL的Schema的标量类型有Int ， Float ， String ， Boolean 和 ID 。还有个 Int64 标量，以及一个以 RFC3339 格式表示的字符串类型的 DateTime 标量类型。而GraphQL的一个schema可以导致生成DQL schema中的多个断言以及相关的DQL type，具体如下图所示。  
      ![alt text](ZZLMdPictures/image.png)  
      PS:Scalar Type（标量类型）是数据库中的一种基本数据类型，用于表示单个值或原子值。  
    
### (5) Dgraph 采取gRPC进行分布式通信，gRPC是什么？
  - 5.1 所谓RPC(remote procedure call 远程过程调用)框架实际是提供了一套机制，使得应用程序之间可以进行通信，而且也遵从server/client模型。使用的时候客户端调用server端提供的接口就像是调用本地的函数一样。  
  - 5.2 gRPC和restful API都提供了一套通信机制，用于server/client模型通信，而且它们都使用http作为底层的传输协议(严格地说, gRPC使用的http2.0，而restful api则不一定)。gRPC可以通过protobuf来定义接口，从而可以有更加严格的接口约束条件，且其通过protobuf可以将数据序列化为二进制编码，这会大幅减少需要传输的数据量，从而大幅提高性能。  
  - 5.3 gRPC可以方便地支持流式通信(理论上通过http2.0就可以使用streaming模式, 但是通常web服务的restful api似乎很少这么用，通常的流式数据应用如视频流，一般都会使用专门的协议如HLS，RTMP等，这些就不是我们通常web服务了，而是有专门的服务器应用。）  

### (6) go语言的一些特性
  - 6.1 结构体后面跟的``内的内容是结构体标签，是一种元数据类型，用于控制操作如何进行的  
      详见 https://www.cnblogs.com/aresxin/p/go-label.html  
      
      


### 看官网，摘出来有用的散知识
1.对于多节点设置，谓词被分配给首先看到该谓词的组。Dgraph 还会自动将谓词数据移动到不同的组以平衡谓词分布。这会每 10 分钟自动发生一次。客户端可以通过与所有 Dgraph 实例通信来协助此过程。对于 Go 客户端，这意味着为每个 Dgraph 实例传递一个 *grpc.ClientConn ，或者通过负载均衡器路由流量。变更以轮询方式执行，导致半随机的初始谓词分布。
2.

<picture>
      <source
        srcset="/logo-dark.png"
        media="(prefers-color-scheme: dark)"
      />
      <source
        srcset="/logo.png"
        media="(prefers-color-scheme: light), (prefers-color-scheme: no-preference)"
      />
      <img alt="Dgraph Logo" src="/logo.png">
</picture>

**The Only Native GraphQL Database With A Graph Backend.**

[![Wiki](https://img.shields.io/badge/res-wiki-blue.svg)](https://dgraph.io/docs/)
[![ci-dgraph-tests](https://github.com/hypermodeinc/dgraph/actions/workflows/ci-dgraph-tests.yml/badge.svg)](https://github.com/hypermodeinc/dgraph/actions/workflows/ci-dgraph-tests.yml)
[![ci-dgraph-load-tests](https://github.com/hypermodeinc/dgraph/actions/workflows/ci-dgraph-load-tests.yml/badge.svg)](https://github.com/hypermodeinc/dgraph/actions/workflows/ci-dgraph-load-tests.yml)
[![ci-golang-lint](https://github.com/hypermodeinc/dgraph/actions/workflows/ci-golang-lint.yml/badge.svg)](https://github.com/hypermodeinc/dgraph/actions/workflows/ci-golang-lint.yml)
[![ci-aqua-security-trivy-tests](https://github.com/hypermodeinc/dgraph/actions/workflows/ci-aqua-security-trivy-tests.yml/badge.svg)](https://github.com/hypermodeinc/dgraph/actions/workflows/ci-aqua-security-trivy-tests.yml)
[![Coverage Status](https://coveralls.io/repos/github/hypermodeinc/dgraph/badge.svg?branch=main)](https://coveralls.io/github/hypermodeinc/dgraph?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/hypermodeinc/dgraph)](https://goreportcard.com/report/github.com/hypermodeinc/dgraph)
[![TODOs](https://badgen.net/https/api.tickgit.com/badgen/github.com/hypermodeinc/dgraph/main)](https://www.tickgit.com/browse?repo=github.com/hypermodeinc/dgraph&branch=main)

Dgraph is a horizontally scalable and distributed GraphQL database with a graph backend. It provides
ACID transactions, consistent replication, and linearizable reads. It's built from the ground up to
perform a rich set of queries. Being a native GraphQL database, it tightly controls how the data is
arranged on disk to optimize for query performance and throughput, reducing disk seeks and network
calls in a cluster.

Dgraph's goal is to provide Google production-level scale and throughput, with low enough latency to
serve real-time user queries over terabytes of structured data. Dgraph supports
[GraphQL query syntax](https://dgraph.io/docs/main/query-language/), and responds in
[JSON](http://www.json.org/) and [Protocol Buffers](https://developers.google.com/protocol-buffers/)
over [GRPC](http://www.grpc.io/) and HTTP. Dgraph is written using the Go Programming Language.

## Status

Dgraph is at [version v24.0.5][rel] and is production-ready. Apart from the vast open source
community, it is being used in production at multiple Fortune 500 companies, and by
[Intuit Katlas](https://github.com/intuit/katlas) and
[VMware Purser](https://github.com/vmware/purser). A hosted version of Dgraph is available at
[https://cloud.dgraph.io](https://cloud.dgraph.io).

[rel]: https://github.com/hypermodeinc/dgraph/releases/tag/v24.0.5

## Supported Platforms

Dgraph officially supports the Linux/amd64 architecture. Support for Linux/arm64 is in development.
In order to take advantage of memory performance gains and other architecture-specific advancements
in Linux, we dropped official support Mac and Windows in 2021, see
[this blog post](https://discuss.dgraph.io/t/dropping-support-for-windows-and-mac/12913) for more
information. You can still build and use Dgraph on other platforms (for live or bulk loading for
instance), but support for platforms other than Linux/amd64 is not available.

Running Dgraph in a Docker environment is the recommended testing and deployment method.

## Install with Docker

If you're using Docker, you can use the
[official Dgraph image](https://hub.docker.com/r/dgraph/dgraph/).

```bash
docker pull dgraph/dgraph:latest
```

For more information on a variety Docker deployment methods including Docker Compose and Kubernetes,
see the [docs](https://dgraph.io/docs/installation/single-host-setup/#docker).

## Run a Quick Standalone Cluster

```bash
docker run -it -p 8080:8080 -p 9080:9080 -v ~/dgraph:/dgraph dgraph/standalone:latest
```

## Install from Source

If you want to install from source, install Go 1.19+ or later and the following dependencies:

### Ubuntu

```bash
sudo apt-get update
sudo apt-get install build-essential
```

### Build and Install

Then clone the Dgraph repository and use `make install` to install the Dgraph binary in the
directory named by the GOBIN environment variable, which defaults to $GOPATH/bin or $HOME/go/bin if
the GOPATH environment variable is not set.

```bash
git clone https://github.com/hypermodeinc/dgraph.git
cd dgraph
make install
```

## Get Started

**To get started with Dgraph, follow:**

- Installation to queries in 3 steps via [dgraph.io/docs/](https://dgraph.io/docs/get-started/).
- A longer interactive tutorial via [dgraph.io/tour/](https://dgraph.io/tour/).
- Tutorial and presentation videos on
  [YouTube channel](https://www.youtube.com/channel/UCghE41LR8nkKFlR3IFTRO4w/featured).

## Is Dgraph the right choice for me?

- Do you have more than 10 SQL tables connected via foreign keys?
- Do you have sparse data, which doesn't elegantly fit into SQL tables?
- Do you want a simple and flexible schema, which is readable and maintainable over time?
- Do you care about speed and performance at scale?

If the answers to the above are YES, then Dgraph would be a great fit for your application. Dgraph
provides NoSQL like scalability while providing SQL like transactions and the ability to select,
filter, and aggregate data points. It combines that with distributed joins, traversals, and graph
operations, which makes it easy to build applications with it.

## Dgraph compared to other graph DBs

| Features                            | Dgraph                        | Neo4j                                                    | Janus Graph                           |
| ----------------------------------- | ----------------------------- | -------------------------------------------------------- | ------------------------------------- |
| Architecture                        | Sharded and Distributed       | Single server (+ replicas in enterprise)                 | Layer on top of other distributed DBs |
| Replication                         | Consistent                    | None in community edition (only available in enterprise) | Via underlying DB                     |
| Data movement for shard rebalancing | Automatic                     | Not applicable (all data lies on each server)            | Via underlying DB                     |
| Language                            | GraphQL inspired              | Cypher, Gremlin                                          | Gremlin                               |
| Protocols                           | Grpc / HTTP + JSON / RDF      | Bolt + Cypher                                            | Websocket / HTTP                      |
| Transactions                        | Distributed ACID transactions | Single server ACID transactions                          | Not typically ACID                    |
| Full-Text Search                    | Native support                | Native support                                           | Via External Indexing System          |
| Regular Expressions                 | Native support                | Native support                                           | Via External Indexing System          |
| Geo Search                          | Native support                | External support only                                    | Via External Indexing System          |
| License                             | Apache 2.0                    | GPL v3                                                   | Apache 2.0                            |

## Users

- **Dgraph official documentation is present at [dgraph.io/docs/](https://dgraph.io/docs/).**
- For feature requests or questions, visit [https://discuss.dgraph.io](https://discuss.dgraph.io).
- Check out [the demo at dgraph.io](http://dgraph.io) and
  [the visualization at play.dgraph.io](http://play.dgraph.io/).
- Please see [releases tab](https://github.com/hypermodeinc/dgraph/releases) to find the latest
  release and corresponding release notes.
- [See the Roadmap](https://discuss.dgraph.io/t/product-roadmap-2020/8479) for a list of working and
  planned features.
- Read about the latest updates from the Dgraph team [on our blog](https://open.dgraph.io/).
- Watch tech talks on our
  [YouTube channel](https://www.youtube.com/channel/UCghE41LR8nkKFlR3IFTRO4w/featured).

## Developers

- See a list of issues
  [that we need help with](https://github.com/hypermodeinc/dgraph/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22).
- Please see
  [Contributing to Dgraph](https://github.com/hypermodeinc/dgraph/blob/main/CONTRIBUTING.md) for
  guidelines on contributions.

## Client Libraries

The Dgraph team maintains several
[officially supported client libraries](https://dgraph.io/docs/clients/). There are also libraries
contributed by the community
[unofficial client libraries](https://dgraph.io/docs/clients#unofficial-dgraph-clients).

##

## Contact

- Please use [discuss.dgraph.io](https://discuss.dgraph.io) for documentation, questions, feature
  requests and discussions.
- Please use [discuss.dgraph.io](https://discuss.dgraph.io/c/issues/dgraph/38) for filing bugs or
  feature requests.
- Follow us on Twitter [@dgraphlabs](https://twitter.com/dgraphlabs).

ZZLTEST