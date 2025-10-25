package log

import (
	"context"
	pb "edge-beta/proto"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"strings"
	"time"
)

type GRPCWriter struct {
	Client pb.RequestClient
	Conn   *grpc.ClientConn
	Ctx    context.Context
	Host   string
}

func (w *GRPCWriter) Write(p []byte) (int, error) {

	raw := map[string]any{}
	if err := json.Unmarshal(p, &raw); err != nil {
		_, err := w.Client.SendEvent(w.Ctx, &pb.EventLog{
			TimeUnixNano: timestamppb.Now(),
			Severity:     pb.SeverityNumber_INFO,
			SeverityText: "info",
			Body:         string(p),
			Attributes:   map[string]string{},
		})
		if err != nil {
			return 0, err
		}
		return len(p), nil
	}

	level := getStringAny(raw, "level")
	body := getStringAny(raw, "message")
	if body == "" {
		body = getStringAny(raw, "msg")
		if body == "" {
			body = string(p)
		}
	}
	traceID := firstStringAny(raw, "trace_id", "traceID", "traceId")
	spanID := firstStringAny(raw, "span_id", "spanID", "spanId")
	metaErr := firstStringAny(raw, "meta_err")

	skip := map[string]struct{}{
		"level": {}, "message": {}, "msg": {}, "time": {},
		"trace_id": {}, "traceID": {}, "traceId": {},
		"span_id": {}, "spanID": {}, "spanId": {}, "meta_err": {},
	}
	attrs := make(map[string]string, len(raw))
	for k, v := range raw {
		if _, ok := skip[k]; ok {
			continue
		}
		attrs[k] = toString(v)
	}

	sev, sevText := mapLevelStr(level)

	go func() {
		r, err := w.Client.SendEvent(w.Ctx, &pb.EventLog{
			TimeUnixNano: timestamppb.Now(),
			Severity:     sev,
			SeverityText: sevText,
			Body:         body,
			TraceId:      traceID,
			SpanId:       spanID,
			Attributes:   attrs,
			MetaErr:      metaErr,
		})
		if err != nil {
			fmt.Println("Error sending event:", err)
		}
		if r.Code != "200" {
			fmt.Printf("Error sending event: %s", r.Message)
		}
	}()

	return len(p), nil
}

func mapLevelStr(lv string) (pb.SeverityNumber, string) {
	switch strings.ToLower(lv) {
	case "trace":
		return pb.SeverityNumber_TRACE, "trace"
	case "debug":
		return pb.SeverityNumber_DEBUG, "debug"
	case "info":
		return pb.SeverityNumber_INFO, "info"
	case "warn", "warning":
		return pb.SeverityNumber_WARN, "warn"
	case "error":
		return pb.SeverityNumber_ERROR, "error"
	case "fatal", "panic":
		return pb.SeverityNumber_FATAL, "fatal"
	default:
		return pb.SeverityNumber_SEVERITY_UNSPECIFIED, lv
	}
}

func getStringAny(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return toString(v)
	}
	return ""
}

func firstStringAny(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
			return toString(v)
		}
	}
	return ""
}

func toString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64, float32, int, int32, int64, uint, uint32, uint64, bool:
		return fmt.Sprint(t)
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func InitWriter(grpcHost string) (GRPCWriter, error) {
	conn, err := grpc.Dial(grpcHost, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Error connect to logger, error: %v", err)
		return GRPCWriter{}, err
	} else {
		fmt.Println("Connected to logger")
	}
	var writer = &GRPCWriter{
		Client: pb.NewRequestClient(conn),
		Conn:   conn,
		Ctx:    context.Background(),
	}
	return *writer, nil

}
func (w *GRPCWriter) MonitoringConn() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			s := w.Conn.GetState()
			if s != connectivity.Ready {
				log.Printf("connection not ready, state: %v", s)
				w.Conn.Connect()
			}
		case <-w.Ctx.Done():
			return
		}
	}
}
