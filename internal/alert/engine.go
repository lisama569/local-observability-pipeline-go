// Package alert 实现告警状态机：按关联键（fingerprint）聚合跨信号事件，
// 支持触发、抑制、恢复与重新打开；状态迁移确定性可重放，避免乱序/重启导致抖动。
package alert

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

// State 表示单个 (rule, fingerprint) 的告警状态。
type State string

const (
	StateOK       State = "ok"       // 正常
	StateFiring   State = "firing"   // 触发中（重复触发被抑制）
	StateResolved State = "resolved" // 已恢复（重开窗口内再触发记为 reopen）
)

// Rule 定义一条聚合告警规则。
type Rule struct {
	Name         string        // 规则名
	Window       time.Duration // 聚合窗口（事件时间）
	Threshold    int           // 窗口内 Error 级事件数达到该值则触发
	ResolveAfter time.Duration // 水位线超过最近触发该时长后恢复
	ReopenWindow time.Duration // 恢复后该时长内再次触发记为 reopen 而非新告警
}

// Transition 是一次告警状态迁移，携带判定依据，可追踪、可解释。
type Transition struct {
	Key         string    // 迁移键：rule|fingerprint|to|triggerEventID，幂等且可重放
	Rule        string    // 规则名
	Fingerprint string    // 关联键
	From        State     // 迁移前状态
	To          State     // 迁移后状态
	Reason      string    // 判定依据（如 "3 error events in 1m0s window"）
	TriggerIDs  []string  // 触发本次迁移的事件 ID 列表
	At          time.Time // 迁移发生的事件时间
	Reopened    bool      // 是否为恢复后重开
}

// group 是单个 (rule, fingerprint) 的运行状态。
type group struct {
	state         State
	window        []model.Event // 窗口内的 Error 级事件（按事件时间）
	watermark     time.Time     // 已见最大事件时间，只前进不后退（防乱序抖动）
	lastTriggerAt time.Time     // 最近一次触发的事件时间
	resolvedAt    time.Time     // 最近一次恢复的事件时间
	suppressed    int           // firing 期间被抑制的重复触发次数
}

// Engine 按事件时间推进的告警引擎，线程安全。
type Engine struct {
	mu     sync.Mutex
	rules  []Rule
	groups map[string]*group // key: rule|fingerprint
}

// New 创建告警引擎。
func New(rules ...Rule) *Engine {
	return &Engine{
		rules:  append([]Rule(nil), rules...),
		groups: map[string]*group{},
	}
}

func groupKey(rule, fp string) string { return rule + "\x00" + fp }

// Observe 摄入一个已去重事件，返回由此产生的状态迁移（可能为空）。
// 乱序事件按事件时间参与窗口判定；水位线只前进，恢复判定不受旧事件回流影响，
// 迁移键幂等，按相同顺序重放必然得到相同迁移序列。
func (e *Engine) Observe(ev model.Event) []Transition {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []Transition
	for _, rule := range e.rules {
		key := groupKey(rule.Name, ev.Fingerprint)
		g := e.groups[key]
		if g == nil {
			g = &group{state: StateOK}
			e.groups[key] = g
		}
		out = append(out, e.observe(rule, g, ev)...)
	}
	return out
}

func (e *Engine) observe(rule Rule, g *group, ev model.Event) []Transition {
	var out []Transition
	// 1. Error 级事件进入聚合窗口（跨 metric/log/trace，只要 fingerprint 相同）。
	if ev.Severity >= model.SeverityError {
		pos := sort.Search(len(g.window), func(i int) bool {
			return g.window[i].Timestamp.After(ev.Timestamp)
		})
		g.window = append(g.window, model.Event{})
		copy(g.window[pos+1:], g.window[pos:])
		g.window[pos] = ev
	}
	// 2. 水位线只前进不后退：乱序旧事件不会把状态拉回过去。
	if ev.Timestamp.After(g.watermark) {
		g.watermark = ev.Timestamp
	}
	// 3. 按水位线裁剪窗口。
	cutoff := g.watermark.Add(-rule.Window)
	keep := g.window[:0]
	for _, w := range g.window {
		if !w.Timestamp.Before(cutoff) {
			keep = append(keep, w)
		}
	}
	g.window = keep

	// 4. 触发判定：窗口内 Error 数达到阈值。
	if len(g.window) >= rule.Threshold {
		switch g.state {
		case StateFiring:
			// 抑制：同一故障持续触发不重复告警，仅计数。
			g.suppressed++
			g.lastTriggerAt = g.watermark
		default:
			from := g.state
			reopened := from == StateResolved && !g.resolvedAt.IsZero() &&
				!g.watermark.After(g.resolvedAt.Add(rule.ReopenWindow))
			ids := make([]string, 0, len(g.window))
			for _, w := range g.window {
				ids = append(ids, w.ID)
			}
			to := StateFiring
			t := Transition{
				Key:         transitionKey(rule.Name, ev.Fingerprint, to, ev.ID),
				Rule:        rule.Name,
				Fingerprint: ev.Fingerprint,
				From:        from,
				To:          to,
				Reason: fmt.Sprintf("%d error events in %s window (threshold %d)",
					len(g.window), rule.Window, rule.Threshold),
				TriggerIDs: ids,
				At:         g.watermark,
				Reopened:   reopened,
			}
			g.state = StateFiring
			g.lastTriggerAt = g.watermark
			g.resolvedAt = time.Time{}
			out = append(out, t)
		}
		return out
	}

	// 5. 恢复判定：水位线超过最近触发 ResolveAfter 且窗口已低于阈值。
	if g.state == StateFiring && !g.lastTriggerAt.IsZero() &&
		g.watermark.Sub(g.lastTriggerAt) >= rule.ResolveAfter {
		t := Transition{
			Key:         transitionKey(rule.Name, ev.Fingerprint, StateResolved, ev.ID),
			Rule:        rule.Name,
			Fingerprint: ev.Fingerprint,
			From:        StateFiring,
			To:          StateResolved,
			Reason: fmt.Sprintf("no trigger for %s (resolve_after %s)",
				g.watermark.Sub(g.lastTriggerAt), rule.ResolveAfter),
			TriggerIDs: []string{ev.ID},
			At:         g.watermark,
		}
		g.state = StateResolved
		g.resolvedAt = g.watermark
		out = append(out, t)
	}
	return out
}

// transitionKey 生成幂等迁移键：同一触发事件重复评估得到相同键。
func transitionKey(rule, fp string, to State, triggerID string) string {
	return strings.Join([]string{rule, fp, string(to), triggerID}, "|")
}

// Snapshot 返回当前所有 (rule, fingerprint) 的状态，用于查询与测试。
// map 键格式为 "rule|fingerprint"。
func (e *Engine) Snapshot() map[string]State {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]State, len(e.groups))
	for k, g := range e.groups {
		out[strings.ReplaceAll(k, "\x00", "|")] = g.state
	}
	return out
}
