package lettuce

const (
	// clientGRPC configuration path.
	//
	// String: <root>.lettuce.client.grpc
	clientGRPC = ".lettuce.client.grpc"

	// GRPCKeepAlive configuration path.
	//
	// String: <root>.lettuce.client.grpc.keepAlive
	GRPCKeepAlive = clientGRPC + ".keepAlive"

	// GRPCKeepAliveTime configuration path.
	//
	// String: <root>.lettuce.client.grpc.keepAlive.time
	GRPCKeepAliveTime = GRPCKeepAlive + ".time"

	// GRPCKeepAliveTimeout configuration path.
	//
	// String: <root>.lettuce.client.grpc.keepAlive.timeout
	GRPCKeepAliveTimeout = GRPCKeepAlive + ".timeout"

	// GRPCKeepAlivePermitWithoutStream configuration path.
	//
	// String: <root>.lettuce.client.grpc.keepAlive.permitWithoutStream
	GRPCKeepAlivePermitWithoutStream = GRPCKeepAlive + ".permitWithoutStream"

	// GRPCMessageSizeMax configuration path.
	//
	// String: <root>.lettuce.client.grpc.messageSizeMax
	GRPCMessageSizeMax = clientGRPC + ".messageSizeMax"

	// GRPCMessageSizeMaxReceive configuration path.
	//
	// String: <root>.lettuce.client.grpc.messageSizeMax.receive
	GRPCMessageSizeMaxReceive = GRPCMessageSizeMax + ".receive"

	// GRPCMessageSizeMaxSend configuration path.
	//
	// String: <root>.lettuce.client.grpc.messageSizeMax.send
	GRPCMessageSizeMaxSend = GRPCMessageSizeMax + ".send"

	// GRPCSecurity configuration path.
	//
	// String: <root>.lettuce.client.grpc.security
	GRPCSecurity = clientGRPC + ".security"

	// GRPCSecurityTLS configuration path.
	//
	// String: <root>.lettuce.client.grpc.security.tls
	GRPCSecurityTLS = GRPCSecurity + ".tls"

	// GRPCSecurityTLSEnable configuration path.
	//
	// String: <root>.lettuce.client.grpc.security.tls.enable
	GRPCSecurityTLSEnable = GRPCSecurityTLS + ".enable"

	// GRPCSecurityTLSCertFile configuration path.
	//
	// String: <root>.lettuce.client.grpc.security.tls.certFile
	GRPCSecurityTLSCertFile = GRPCSecurityTLS + ".certFile"

	// GRPCSecurityTLSKeyFile configuration path.
	//
	// String: <root>.lettuce.client.grpc.security.tls.keyFile
	GRPCSecurityTLSKeyFile = GRPCSecurityTLS + ".keyFile"

	// SOCKS5Enable configuration path.
	//
	// value: <root>.lettuce.client.socks5.enable
	SOCKS5Enable = ".lettuce.client.socks5.enable"

	// seaweedFS configuration Path.
	//
	// String: <root>.lettuce.seaweedfs
	seaweedFS = ".lettuce.seaweedfs"

	// SeaweedFSCluster configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.cluster
	SeaweedFSCluster = seaweedFS + ".cluster"

	// SeaweedFSClusterLocal configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.cluster.local
	SeaweedFSClusterLocal = SeaweedFSCluster + ".local"

	// SeaweedFSClusterFiler configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.cluster.filer
	SeaweedFSClusterFiler = SeaweedFSCluster + ".filer"

	// SeaweedFSClusterFilerAddr configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.cluster.filer.address
	SeaweedFSClusterFilerAddr = SeaweedFSClusterFiler + ".address"

	// SeaweedFSClusterMaster configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.cluster.master
	SeaweedFSClusterMaster = SeaweedFSCluster + ".master"

	// SeaweedFSClusterMasterAddr configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.cluster.master.address
	SeaweedFSClusterMasterAddr = SeaweedFSClusterMaster + ".address"

	// SeaweedFSWatcher configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.
	SeaweedFSWatcher = seaweedFS + ".watcher"

	// SeaweedFSWatcherEnable configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.watcher.enable
	SeaweedFSWatcherEnable = SeaweedFSWatcher + ".enable"

	// SeaweedFSWatcherNamespaceExcludes configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.watcher.namespace.excludes
	SeaweedFSWatcherNamespaceExcludes = SeaweedFSWatcher + ".namespace.excludes"

	// SeaweedFSWatcherTimeOffsetBegin configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.watcher.timeOffset.begin
	SeaweedFSWatcherTimeOffsetBegin = SeaweedFSWatcher + ".timeOffset.begin"

	// SeaweedFSWatcherTimeOffsetEnd configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.watcher.timeOffset.end
	SeaweedFSWatcherTimeOffsetEnd = SeaweedFSWatcher + ".timeOffset.end"

	// SeaweedFSWatcherSubscription configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.watcher.subscription
	SeaweedFSWatcherSubscription = SeaweedFSWatcher + ".subscription"

	// SeaweedFSWatcherWriterChunkBufferSize configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.watcher.writer.chunkBufferSize
	SeaweedFSWatcherWriterChunkBufferSize = SeaweedFSWatcher + ".writer.chunkBufferSize"

	// SeaweedFSWatcherWriterConcurrency configuration Path.
	//
	// String: <root>.lettuce.seaweedfs.watcher.writer.concurrency
	SeaweedFSWatcherWriterConcurrency = SeaweedFSWatcher + ".writer.concurrency"
)
