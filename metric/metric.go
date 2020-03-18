//  Copyright (c) 2020 Minoru Osuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metric

import (
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Create a metrics registry.
	Registry = prometheus.NewRegistry()

	// Create some standard server metrics.
	GrpcMetrics = grpcprometheus.NewServerMetrics()
)

func init() {
	// Register standard server metrics and customized metrics to registry.
	Registry.MustRegister(GrpcMetrics)
	GrpcMetrics.EnableHandlingTimeHistogram()
	//reg.MustRegister(grpcMetrics, customizedCounterMetric)
	//customizedCounterMetric.WithLabelValues("Test")
}

//var (
//	namespace     = "cete"
//	grpcSubsystem = "grpc"
//	httpSubsystem = "http"
//	kvsSubsystem  = "kvs"
//
//	GrpcDurationSeconds = prometheus.NewHistogramVec(
//		prometheus.HistogramOpts{
//			Namespace: namespace,
//			Subsystem: grpcSubsystem,
//			Name:      "duration_seconds",
//			Help:      "The index operation durations in seconds.",
//		},
//		[]string{
//			"method",
//		},
//	)
//	GrpcOperationsTotal = prometheus.NewCounterVec(
//		prometheus.CounterOpts{
//			Namespace: namespace,
//			Subsystem: grpcSubsystem,
//			Name:      "operations_total",
//			Help:      "The number of index operations.",
//		},
//		[]string{
//			"method",
//		},
//	)
//
//	HttpDurationSeconds = prometheus.NewHistogramVec(
//		prometheus.HistogramOpts{
//			Namespace: namespace,
//			Subsystem: httpSubsystem,
//			Name:      "duration_seconds",
//			Help:      "The index operation durations in seconds.",
//		},
//		[]string{
//			"method",
//			"uri",
//			"protocol",
//			"referer",
//			"user_agent",
//		},
//	)
//	HttpRequestsTotal = prometheus.NewCounterVec(
//		prometheus.CounterOpts{
//			Namespace: namespace,
//			Subsystem: httpSubsystem,
//			Name:      "requests_total",
//			Help:      "The number of index operations.",
//		},
//		[]string{
//			"method",
//			"uri",
//			"protocol",
//			"referer",
//			"user_agent",
//		},
//	)
//	HttpResponsesTotal = prometheus.NewCounterVec(
//		prometheus.CounterOpts{
//			Namespace: namespace,
//			Subsystem: httpSubsystem,
//			Name:      "responses_total",
//			Help:      "The number of index operations.",
//		},
//		[]string{
//			"method",
//			"uri",
//			"protocol",
//			"referer",
//			"user_agent",
//			"status",
//		},
//	)
//)
//
//func init() {
//	prometheus.MustRegister(GrpcDurationSeconds)
//	prometheus.MustRegister(GrpcOperationsTotal)
//
//	prometheus.MustRegister(HttpDurationSeconds)
//	prometheus.MustRegister(HttpRequestsTotal)
//}
//
//func RecordGrpcMetrics(start time.Time, funcName string) {
//	GrpcDurationSeconds.With(
//		prometheus.Labels{
//			"method": funcName,
//		},
//	).Observe(float64(time.Since(start)) / float64(time.Second))
//
//	GrpcOperationsTotal.With(
//		prometheus.Labels{
//			"method": funcName,
//		},
//	).Inc()
//
//	return
//}
//
////func RecordHttpMetrics(start time.Time, method string, uri string) {
////	HttpDurationSeconds.With(
////		prometheus.Labels{
////			"method": method,
////			"uri":    uri,
////		},
////	).Observe(float64(time.Since(start)) / float64(time.Second))
////
////	HttpOperationsTotal.With(
////		prometheus.Labels{
////			"method": method,
////			"uri":    uri,
////		},
////	).Inc()
////
////	return
////}
