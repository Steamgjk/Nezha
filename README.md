# Nezhav2

Nezha: Deployable and High-Performance Consensus Using Synchronized Clocks [[preprint](https://arxiv.org/pdf/2206.03285.pdf)]


An early presentation of Nezha was made at [Stanford Platform Lab Winter Review 2022](https://platformlab.stanford.edu/winter-review/platform-lab-winter-review-2022/) [[slides](https://platformlab.stanford.edu/wp-content/uploads/2022/03/Jinkun-Geng.pdf)]

## Clone Project

```
git clone --depth=1 https://gitlab.com/steamgjk/nezhav2.git
```


## File Structure
The core part includes three modules (folders), i.e., 
- replica
- proxy
- client 

Each module is composed of three files: 
- a header file (e.g., replica.h), 
- a source implementation file (replica.cc), 
- a launching file (e.g., replica_run.cc). 

Each process reads an independent yaml file (e.g., nezha-replica-config-0.yaml) to get its full configuration, the sample configuration files are placed in the configs folder



## Install Dependencies

We have packaged the installation of all depencies into one script. Please check [install-dep.sh](./scripts/install-dep.sh) for the detailed dependencies we require.

```
./scripts/install-dep.sh
```

## Build Nezha with Bazel

Since Bazel is becoming popular, we have migrated nezha from Makefile-based building system to the bazel building system. The bazel version in use is 5.2.0

```
cd nezhav2 && bazel build //...
```


After building the project successfully, the executable files will be generated in the folder named bazel-bin



## Single-Machine Tests

Please refer to [the single-machine instructions](docs/demo.md) to run Nezha under various scenarios (view change, request commit, recovery from failure of replica).

## Multi-Machine Tests

We use [scripts/launch.py](scripts/launch.py) to conduct distributed tests across multiple machines. After the tests have completed, [scripts/analysis.py](scripts/analysis.py) is used to analyze the results to generate performance numbers. The current scripts only support Google Cloud Platform (GCP). They require GCP credentials to create and delete VMs on GCP.


## Important Configuration Parameters
### Replica
- ```replica-ips``` must include 2f+1 ips
- ```replica-id``` starts from 0 to 2f
- ```index-transfer-batch```, ```request-key-transfer-batch```, ```request-transfer-batch```. The values of the three <em>batch parameters</em> should be carefully chosen in order not to overflow the [maximum size of UDP packets](https://stackoverflow.com/questions/1098897/what-is-the-largest-safe-udp-packet-size-on-the-internet). 

### Clients
- We support two types of clients, i.e., open-loop clients and closed-loop clients.
- Open-loop clients generate requests according to a Poisson process configured with a specific rate.
- Closed-loop clients use a sliding window protocol to keep a fixed number of requests in flight at any given time, release a new request when an old one is completed.
- ```is-openloop```:  When this flag is true, --poission-rate becomes meaningful.
- ```skew-factor``` and key-number decides the workload, which further affects the commutativity optimization

### Proxy
- ```shard-num``` decides how many threads will be launched. 1 shard includes 1 forwarding thread to forward client requests to replicas and 1 replying thread to receive and replies from replicas and does quorum check
- ```max-owd```  is used in the clamping function to estimate one-way delay, more details are described in Sec 4 [Adpative latency bound] of the paper.

## Performance Benchmark
Refer to [our paper](https://arxiv.org/pdf/2206.03285.pdf) for the relevant performance stats. Compared with the experimental version, we have refactored the codebase with some higher-performance libraries (e.g. libev instead of libevent) and data structures (e.g., ConcurrentMap and ConcurrentQueue). Besides, we have also conducted further optimization with the pipeline. The performance will be somewhat better than the original version used in the paper. New benchmark data will be updated soon. 


## Authors and Acknowledgment
Nezha project is developed and maintained by [Jinkun Geng](https://steamgjk.github.io/) and his three supervisors, i.e., [Prof. Anirudh Sivaraman](https://cs.nyu.edu/~anirudh/), [Prof. Balaji Prabhakar](https://web.stanford.edu/~balaji/) and [Prof. Mendel Rosenblum](http://web.stanford.edu/~mendel/).

We are fortunate to get the help from many researchers during the development of Nezha. Below we list and acknowledge them according to the timeline.

[Dr. Shiyu Liu](https://web.stanford.edu/~shiyuliu/) and [Dr. Feiran Wang](https://www.linkedin.com/in/feiran-wang/) joined the early discussion of Nezha. Feiran explained the details of Craft and Shiyu explained Huygens and other clock sync solutions.

[Prof. Dan Ports](https://drkp.net/), [Prof. Jialin Li](https://www.comp.nus.edu.sg/~lijl/) and [Dr. Ellis Michael](https://ellismichael.com/) provided helpful discussion related to Speculative Paxos and NOPaxos. Dan also gave us the pointer to crash vector and diskless recovery. 

[Prof. Jinyang Li](http://www.news.cs.nyu.edu/~jinyang/) listened to our early presentation of Nezha, and gave some useful feedback.

[Prof. Seo Jin Park](https://seojinpark.net/) discussed with us about the definition of linearizability. Seo Jin also provided some explanation about CURP.

[Prof. Zhaoguo Wang](https://ipads.se.sjtu.edu.cn/pub/members/zhaoguo_wang) shared with us his experience in testing Raft.

The [Derecho team](https://derecho-project.github.io/) (Prof. Ken Birman, Dr. Weijia Song, Dr. Sagar Jha, Dr. Lorenzo Rosa, etc) offered technical support and discussion during our measurement of Derecho.

The [ClockWork](https://www.clockwork.io/) Staff (Dr. Yilong Geng and Dr. Deepak Merugu) offered technical support in deploying Huygens. Dr. Deepak Merugu also gave suggestions on the coding-styles of Nezha codebase. Katie Gioioso provided feedback on Nezha design. Bhagirath Mehta participated in the single-machine test of Nezha.

[Prof. Eugene Wu](http://www.cs.columbia.edu/~ewu/) provided suggestions on the revision of Nezha paper.

[Prof. Aurojit Panda](https://cs.nyu.edu/~apanda/) discussed with us about Nezha's correctness during leader change. Aurojit reviewed our draft and offered some constructive suggestions on the revision.




## License
Please refer to [license.md](license.md)

## Future Plan

(1) Conduct more functionality and performance tests to make Nezha more robust and optimized

(3) Support grpc-based communication primitive, and further replace [the etcd backend for Kubenetes](https://learnk8s.io/etcd-kubernetes).

