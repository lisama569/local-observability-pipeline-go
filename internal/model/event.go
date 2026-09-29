// Package model 定义统一的事件模型：指标、日志、追踪三类事件
// 归一化为同一个 Event 结构，附带校验与去重键规则。
package model

import (
	"errors"
	"time"
)

// Kind 表示事件类型。
type Kind string

const (
	KindMetric Kind = "metric"
	KindLog    Kind = "log"
	KindTrace  Kind = "trace"
)

// Severity 表示事件严重级别，告警聚合按级别统计。
type Severity int

const (
	SeverityInfo Severity = iota
	SeverityWarn
	SeverityError
)

func (s Severity) String() string {
	switch s {
	case SeverityInfo:
		return "info"
	case SeverityWarn:
		return "warn"
	case SeverityError:
		return "error"
	}
	return "unknown"
}

// 可区分的校验失败原因。
var (
	ErrMissingID          = errors.New("model: missing event id")
	ErrMissingKind        = errors.New("model: missing or unknown event kind")
	ErrMissingSource      = errors.New("model: missing source")
	ErrMissingFingerprint = errors.New("model: missing fingerprint")
	ErrMissingTimestamp   = errors.New("model: missing timestamp")
)

// Event 是归一化后的统一事件。
type Event struct {
	ID          string            // 生产侧唯一 ID，用于幂等去重
	Kind        Kind              // metric / log / trace
	Source      string            // 来源（服务/主机）
	Fingerprint string            // 关联键：同一故障的跨信号关联落在此键上
	Timestamp   time.Time         // 事件发生时间（事件时间）
	Severity    Severity          // 严重级别
	Message     string            // 文本内容（日志/追踪描述/指标说明）
	Value       float64           // 指标值（非指标事件为 0）
	TraceID     string            // 追踪 ID（可空）
	Attributes  map[string]string // 附加属性
}

// DedupKey 返回幂等去重键：同一来源 + 同一事件 ID 视为重复投递。
func (e Event) DedupKey() string { return e.Source + "\x00" + e.ID }

// Validate 校验必填字段，返回可区分的错误原因（errors.Is 可判定）。
func (e Event) Validate() error {
	if e.ID == "" {
		return ErrMissingID
	}
	switch e.Kind {
	case KindMetric, KindLog, KindTrace:
	default:
		return ErrMissingKind
	}
	if e.Source == "" {
		return ErrMissingSource
	}
	if e.Fingerprint == "" {
		return ErrMissingFingerprint
	}
	if e.Timestamp.IsZero() {
		return ErrMissingTimestamp
	}
	return nil
}

// Normalize 补齐缺失的可选字段，并返回被补齐的字段名（保证结果可判别）。
// 规则：Attributes 为 nil 时补空 map；trace 事件缺 TraceID 时用事件 ID 顶替。
func (e *Event) Normalize() (defaulted []string) {
	if e.Attributes == nil {
		e.Attributes = map[string]string{}
		defaulted = append(defaulted, "attributes")
	}
	if e.Kind == KindTrace && e.TraceID == "" {
		e.TraceID = e.ID
		defaulted = append(defaulted, "trace_id")
	}
	return defaulted
}
