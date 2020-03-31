package metric

import (
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Create a metrics registry.
	Registry = prometheus.NewRegistry()

	// Create some standard server metrics.
	GrpcMetrics = grpcprometheus.NewServerMetrics(
		func(o *prometheus.CounterOpts) {
			o.Namespace = "cete"
		},
	)

	// Raft node state metric
	RaftStateMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "state",
		Help:      "Node state. 0:Follower, 1:Candidate, 2:Leader, 3:Shutdown",
	}, []string{"id"})

	RaftTermMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "term",
		Help:      "Term.",
	}, []string{"id"})

	RaftLastLogIndexMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "last_log_index",
		Help:      "Last log index.",
	}, []string{"id"})

	RaftLastLogTermMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "last_log_term",
		Help:      "Last log term.",
	}, []string{"id"})

	RaftCommitIndexMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "commit_index",
		Help:      "Commit index.",
	}, []string{"id"})

	RaftAppliedIndexMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "applied_index",
		Help:      "Applied index.",
	}, []string{"id"})

	RaftFsmPendingMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "fsm_pending",
		Help:      "FSM pending.",
	}, []string{"id"})

	RaftLastSnapshotIndexMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "last_snapshot_index",
		Help:      "Last snapshot index.",
	}, []string{"id"})

	RaftLastSnapshotTermMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "last_snapshot_term",
		Help:      "Last snapshot term.",
	}, []string{"id"})

	RaftLatestConfigurationIndexMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "latest_configuration_index",
		Help:      "Latest configuration index.",
	}, []string{"id"})

	RaftNumPeersMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "num_peers",
		Help:      "Number of peers.",
	}, []string{"id"})

	RaftLastContactMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "last_copntact",
		Help:      "Last contact.",
	}, []string{"id"})

	RaftNumNodesMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "raft",
		Name:      "num_nodes",
		Help:      "Number of nodes.",
	}, []string{"id"})

	KvsNumReadsMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "num_reads",
		Help:      "Number of reads.",
	}, []string{"id"})

	KvsNumWritesMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "num_writes",
		Help:      "Number of writes.",
	}, []string{"id"})

	KvsNumBytesReadMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "num_bytes_read",
		Help:      "Number of bytes read.",
	}, []string{"id"})

	KvsNumBytesWrittenMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "num_bytes_written",
		Help:      "Number of bytes written.",
	}, []string{"id"})

	KvsNumLSMGetsMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "num_lsm_gets",
		Help:      "Number of LSM gets.",
	}, []string{"id"})

	KvsNumLSMBloomHitsMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "num_lsm_bloom_Hits",
		Help:      "Number of LSM bloom hits.",
	}, []string{"id"})

	KvsNumGetsMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "num_gets",
		Help:      "Number of gets.",
	}, []string{"id"})

	KvsNumPutsMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "num_puts",
		Help:      "Number of puts.",
	}, []string{"id"})

	KvsNumBlockedPutsMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "num_blocked_puts",
		Help:      "Number of blocked puts.",
	}, []string{"id"})

	KvsNumMemtablesGetsMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "num_memtables_gets",
		Help:      "Number of memtables gets.",
	}, []string{"id"})

	KvsLSMSizeMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "lsm_size",
		Help:      "LSM size.",
	}, []string{"id", "path"})

	KvsVlogSizeMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "vlog_size",
		Help:      "Vlog size.",
	}, []string{"id", "path"})

	KvsPendingWritesMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cete",
		Subsystem: "kvs",
		Name:      "pending_writes",
		Help:      "Pending writes.",
	}, []string{"id", "path"})
)

func init() {
	// Register standard server metrics and customized metrics to registry.
	Registry.MustRegister(
		GrpcMetrics,
		RaftStateMetric,
		RaftTermMetric,
		RaftLastLogIndexMetric,
		RaftLastLogTermMetric,
		RaftCommitIndexMetric,
		RaftAppliedIndexMetric,
		RaftFsmPendingMetric,
		RaftLastSnapshotIndexMetric,
		RaftLastSnapshotTermMetric,
		RaftLatestConfigurationIndexMetric,
		RaftNumPeersMetric,
		RaftLastContactMetric,
		RaftNumNodesMetric,
		KvsNumReadsMetric,
		KvsNumWritesMetric,
		KvsNumBytesReadMetric,
		KvsNumBytesWrittenMetric,
		KvsNumLSMGetsMetric,
		KvsNumLSMBloomHitsMetric,
		KvsNumGetsMetric,
		KvsNumPutsMetric,
		KvsNumBlockedPutsMetric,
		KvsNumMemtablesGetsMetric,
		KvsLSMSizeMetric,
		KvsVlogSizeMetric,
		KvsPendingWritesMetric,
	)
	GrpcMetrics.EnableHandlingTimeHistogram(
		func(o *prometheus.HistogramOpts) {
			o.Namespace = "cete"
		},
	)
}
