package server

import (
	"math"
	"net"
	"time"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/mosuka/cete/metric"
	"github.com/mosuka/cete/protobuf"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type GRPCServer struct {
	address  string
	service  *GRPCService
	server   *grpc.Server
	listener net.Listener

	certFile     string
	keyFile      string
	certHostname string

	logger *zap.Logger
}

func NewGRPCServer(address string, raftServer *RaftServer, certFile string, keyFile string, certHostname string, logger *zap.Logger) (*GRPCServer, error) {
	grpcLogger := logger.Named("grpc")

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(math.MaxInt64),
		grpc.MaxSendMsgSize(math.MaxInt64),
		grpc.StreamInterceptor(
			grpcmiddleware.ChainStreamServer(
				metric.GrpcMetrics.StreamServerInterceptor(),
				grpczap.StreamServerInterceptor(grpcLogger),
			),
		),
		grpc.UnaryInterceptor(
			grpcmiddleware.ChainUnaryServer(
				metric.GrpcMetrics.UnaryServerInterceptor(),
				grpczap.UnaryServerInterceptor(grpcLogger),
			),
		),
		grpc.KeepaliveParams(
			keepalive.ServerParameters{
				//MaxConnectionIdle:     0,
				//MaxConnectionAge:      0,
				//MaxConnectionAgeGrace: 0,
				Time:    5 * time.Second,
				Timeout: 5 * time.Second,
			},
		),
	}

	if certFile == "" && keyFile == "" {
		logger.Info("disabling TLS")
	} else {
		logger.Info("enabling TLS")
		creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
		if err != nil {
			logger.Error("failed to create credentials", zap.Error(err))
		}
		opts = append(opts, grpc.Creds(creds))
	}

	server := grpc.NewServer(
		opts...,
	)

	service, err := NewGRPCService(raftServer, certFile, certHostname, logger)
	if err != nil {
		logger.Error("failed to create key value store service", zap.Error(err))
		return nil, err
	}

	protobuf.RegisterKVSServer(server, service)

	// Initialize all metrics.
	metric.GrpcMetrics.InitializeMetrics(server)
	grpc_prometheus.Register(server)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		logger.Error("failed to create listener", zap.String("address", address), zap.Error(err))
		return nil, err
	}

	return &GRPCServer{
		address:      address,
		service:      service,
		server:       server,
		listener:     listener,
		certFile:     certFile,
		keyFile:      keyFile,
		certHostname: certHostname,
		logger:       logger,
	}, nil
}

func (s *GRPCServer) Start() error {
	if err := s.service.Start(); err != nil {
		s.logger.Error("failed to start service", zap.Error(err))
	}

	go func() {
		_ = s.server.Serve(s.listener)
	}()

	s.logger.Info("gRPC server started", zap.String("addr", s.address))
	return nil
}

func (s *GRPCServer) Stop() error {
	if err := s.service.Stop(); err != nil {
		s.logger.Error("failed to stop service", zap.Error(err))
	}

	//s.server.GracefulStop()
	s.server.Stop()

	s.logger.Info("gRPC server stopped", zap.String("addr", s.address))
	return nil
}
