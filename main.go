package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ─── Config ──────────────────────────────────────────────────────────────────

type QueryConfig struct {
	Name        string
	Query       string
	ZThreshold  float64
	MinSamples  int
	Silenced    bool
	MinDelta    float64 // minimum absolute change to be anomalous (anti false-positive)
	MinRelative float64 // minimum relative change (fraction of mean) to be anomalous
}

type Config struct {
	PrometheusURL   string
	ListenAddr      string
	LookbackDays    int
	WindowMinutes   int
	StepSeconds     int
	ZThreshold      float64
	IntervalSeconds int
	MinSamples      int
	MinDelta        float64
	MinRelative     float64
	Timezone        string
	Queries         []QueryConfig
}

func defaultConfig() *Config {
	return &Config{
		PrometheusURL:   "http://localhost:9090",
		ListenAddr:      "0.0.0.0:9091",
		LookbackDays:    14,
		WindowMinutes:   30,
		StepSeconds:     300,
		ZThreshold:      3.0,
		IntervalSeconds: 300,
		MinSamples:      10,   // raised from 5
		MinDelta:        0.0,  // absolute minimum change
		MinRelative:     0.0,  // relative minimum change
		Timezone:        "UTC",
	}
}

func loadConfig(path string) (*Config, error) {
	cfg := defaultConfig()
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	section := ""
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if idx := strings.Index(line, " #"); idx >= 0 {
			line = strings.TrimSpace(line[:idx])
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(line[1 : len(line)-1])
			continue
		}
		eqIdx := strings.Index(line, "=")
		if eqIdx < 0 {
			continue
		}
		key := strings.TrimSpace(line[:eqIdx])
		val := strings.TrimSpace(line[eqIdx+1:])

		switch section {
		case "general":
			switch key {
			case "prometheus_url":
				cfg.PrometheusURL = val
			case "listen_addr":
				cfg.ListenAddr = val
			case "lookback_days":
				cfg.LookbackDays, _ = strconv.Atoi(val)
			case "window_minutes":
				cfg.WindowMinutes, _ = strconv.Atoi(val)
			case "step_seconds":
				cfg.StepSeconds, _ = strconv.Atoi(val)
			case "z_threshold":
				cfg.ZThreshold, _ = strconv.ParseFloat(val, 64)
			case "interval_seconds":
				cfg.IntervalSeconds, _ = strconv.Atoi(val)
			case "min_samples":
				cfg.MinSamples, _ = strconv.Atoi(val)
			case "min_delta":
				cfg.MinDelta, _ = strconv.ParseFloat(val, 64)
			case "min_relative":
				cfg.MinRelative, _ = strconv.ParseFloat(val, 64)
			case "timezone":
				cfg.Timezone = val
			}
		case "queries":
			parts := strings.Split(val, "|")
			qc := QueryConfig{
				Name:        key,
				Query:       strings.TrimSpace(parts[0]),
				ZThreshold:  0,
				MinSamples:  0,
				MinDelta:    -1,
				MinRelative: -1,
			}
			for _, opt := range parts[1:] {
				opt = strings.TrimSpace(opt)
				kv := strings.SplitN(opt, "=", 2)
				if len(kv) != 2 {
					continue
				}
				k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
				switch k {
				case "z_threshold":
					qc.ZThreshold, _ = strconv.ParseFloat(v, 64)
				case "min_samples":
					qc.MinSamples, _ = strconv.Atoi(v)
				case "silence", "silenced":
					qc.Silenced = v == "true" || v == "1"
				case "min_delta":
					qc.MinDelta, _ = strconv.ParseFloat(v, 64)
				case "min_relative":
					qc.MinRelative, _ = strconv.ParseFloat(v, 64)
				}
			}
			cfg.Queries = append(cfg.Queries, qc)
		}
	}
	if len(cfg.Queries) == 0 {
		return nil, fmt.Errorf("no queries defined in config")
	}
	return cfg, scanner.Err()
}

// ─── Prometheus API ───────────────────────────────────────────────────────────

type promSample struct {
	Metric map[string]string
	Values [][]interface{}
}

func queryRange(ctx context.Context, promURL, query string, start, end time.Time, step int) ([]promSample, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", fmt.Sprintf("%.0f", float64(start.Unix())))
	params.Set("end", fmt.Sprintf("%.0f", float64(end.Unix())))
	params.Set("step", fmt.Sprintf("%d", step))

	reqURL := promURL + "/api/v1/query_range?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var result struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	var out []promSample
	for _, r := range result.Data.Result {
		out = append(out, promSample{Metric: r.Metric, Values: r.Values})
	}
	return out, nil
}

func parseValues(raw [][]interface{}) []float64 {
	var out []float64
	for _, pair := range raw {
		if len(pair) < 2 {
			continue
		}
		s, ok := pair[1].(string)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		out = append(out, v)
	}
	return out
}

// ─── Math ─────────────────────────────────────────────────────────────────────

func meanStddev(vals []float64) (float64, float64) {
	if len(vals) == 0 {
		return 0, 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	mean := sum / float64(len(vals))
	if len(vals) < 2 {
		return mean, 0
	}
	var variance float64
	for _, v := range vals {
		d := v - mean
		variance += d * d
	}
	return mean, math.Sqrt(variance / float64(len(vals)-1))
}

func linearSlope(vals []float64) float64 {
	n := float64(len(vals))
	if n < 2 {
		return 0
	}
	var sumX, sumY, sumXY, sumX2 float64
	for i, v := range vals {
		x := float64(i)
		sumX += x
		sumY += v
		sumXY += x * v
		sumX2 += x * x
	}
	denom := n*sumX2 - sumX*sumX
	if denom == 0 {
		return 0
	}
	return (n*sumXY - sumX*sumY) / denom
}

func spikeScore(vals []float64, stddev float64) float64 {
	if len(vals) < 2 || stddev == 0 {
		return 0
	}
	last := vals[len(vals)-1]
	prev := vals[len(vals)-2]
	return (last - prev) / stddev
}

// anomalousPercent: % of CURRENT window points that deviate beyond threshold
// (fixed: was measuring historical points, now measures current window quality)
func anomalousPercent(currentVals []float64, mean, stddev, threshold float64) float64 {
	if len(currentVals) == 0 || stddev == 0 {
		return 0
	}
	var count int
	for _, v := range currentVals {
		if math.Abs((v-mean)/stddev) > threshold {
			count++
		}
	}
	return float64(count) / float64(len(currentVals)) * 100.0
}

// confidenceFactor: dampens composite score when baseline has few samples
// Returns 1.0 when samples >= 30, scales down linearly below that
func confidenceFactor(samples int) float64 {
	if samples >= 30 {
		return 1.0
	}
	return math.Max(0.3, float64(samples)/30.0)
}

func compositeScore(z, trendNorm, spike, confidence float64) float64 {
	raw := 0.6*math.Abs(z) + 0.2*math.Abs(trendNorm) + 0.2*math.Abs(spike)
	return raw * confidence
}

func severityLabel(score float64) string {
	switch {
	case score >= 6.0:
		return "critical"
	case score >= 4.0:
		return "high"
	case score >= 2.5:
		return "medium"
	case score >= 1.5:
		return "low"
	default:
		return "normal"
	}
}

func isWeekend(t time.Time) bool {
	return t.Weekday() == time.Saturday || t.Weekday() == time.Sunday
}

// seriesKey builds a stable key from metric labels (only metric_name + instance + exported_instance)
func seriesKey(metricName, instance, exportedInstance string) string {
	return metricName + "|" + instance + "|" + exportedInstance
}

// labelString builds a clean sorted label string excluding standard labels
func labelString(m map[string]string) string {
	exclude := map[string]bool{
		"__name__": true, "instance": true, "job": true,
		"metric_name": true, "exported_instance": true,
	}
	var parts []string
	for k, v := range m {
		if !exclude[k] {
			parts = append(parts, k+"="+v)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// ─── Metrics Registry ─────────────────────────────────────────────────────────

// Use only 3 core labels: metric_name, instance, exported_instance
// This eliminates the `labels` label that was causing split rows in Grafana
var coreLabels = []string{"metric_name", "instance", "exported_instance"}
var severityLabels = append(coreLabels, "severity_label")
var stateLabels = append(coreLabels, "state")

var (
	gZScore        = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_z_score", Help: "Z-score vs seasonal baseline"}, coreLabels)
	gPct           = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_percentage", Help: "% of current window points that are anomalous"}, coreLabels)
	gDetected      = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_detected", Help: "1 if anomalous, 0 if normal"}, coreLabels)
	gCurrent       = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_current_value", Help: "Latest value"}, coreLabels)
	gMean          = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_baseline_mean", Help: "Baseline mean"}, coreLabels)
	gStddev        = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_baseline_stddev", Help: "Baseline stddev"}, coreLabels)
	gSamples       = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_baseline_samples", Help: "Number of baseline samples"}, coreLabels)
	gHour          = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_hour_of_day", Help: "Local hour used for baseline"}, coreLabels)
	gSlope         = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_trend_slope", Help: "Linear regression slope"}, coreLabels)
	gSlopeNorm     = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_trend_slope_normalized", Help: "Slope / stddev"}, coreLabels)
	gSpike         = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_spike_score", Help: "Instantaneous jump score"}, coreLabels)
	gComposite     = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_composite_score", Help: "Weighted composite score"}, coreLabels)
	gConfidence    = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_baseline_confidence", Help: "Baseline confidence 0-1 (low when few samples)"}, coreLabels)
	gWeekend       = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_weekend_baseline", Help: "1 if weekend baseline"}, coreLabels)
	gConsecutive   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_consecutive_cycles", Help: "Consecutive anomalous cycles"}, coreLabels)
	gLastAnomaly   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_last_anomaly_timestamp_seconds", Help: "Unix ts of last anomaly"}, coreLabels)
	gSeverity      = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_severity", Help: "Severity label gauge"}, severityLabels)
	gHealthState   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_health_state", Help: "Health state gauge"}, stateLabels)
	gSilenced      = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anomaly_silenced", Help: "1 if silenced"}, []string{"metric_name"})
	gLastScrape    = prometheus.NewGauge(prometheus.GaugeOpts{Name: "anomaly_detector_last_scrape_timestamp_seconds", Help: "Last cycle unix ts"})
	cScrapeErrors  = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "anomaly_detector_scrape_errors_total", Help: "Scrape errors"}, []string{"metric_name"})
)

func init() {
	prometheus.MustRegister(
		gZScore, gPct, gDetected, gCurrent, gMean, gStddev, gSamples, gHour,
		gSlope, gSlopeNorm, gSpike, gComposite, gConfidence, gWeekend,
		gConsecutive, gLastAnomaly, gSeverity, gHealthState,
		gSilenced, gLastScrape, cScrapeErrors,
	)
}

// ─── State Machine ────────────────────────────────────────────────────────────

type MetricState struct {
	mu                 sync.RWMutex
	consecutiveAnomalies map[string]int
	lastAnomalyTime    map[string]time.Time
	prevHealthState    map[string]string
}

var globalState = &MetricState{
	consecutiveAnomalies: make(map[string]int),
	lastAnomalyTime:    make(map[string]time.Time),
	prevHealthState:    make(map[string]string),
}

func (s *MetricState) Update(key string, isAnomaly bool, severity string) (consecutive int, healthState string, lastAnomaly time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if isAnomaly {
		s.consecutiveAnomalies[key]++
		s.lastAnomalyTime[key] = time.Now()
	} else {
		s.consecutiveAnomalies[key] = 0
	}

	consecutive = s.consecutiveAnomalies[key]
	lastAnomaly = s.lastAnomalyTime[key]
	prev := s.prevHealthState[key]

	switch {
	case consecutive >= 3 && (severity == "critical" || severity == "high"):
		healthState = "critical"
	case consecutive >= 1 && (severity == "medium" || severity == "low"):
		healthState = "warning"
	case consecutive >= 1:
		healthState = "warning"
	case consecutive == 0 && (prev == "critical" || prev == "warning"):
		healthState = "recovering"
	default:
		healthState = "normal"
	}

	s.prevHealthState[key] = healthState
	return
}

// ─── Status store ─────────────────────────────────────────────────────────────

type StatusEntry struct {
	MetricName       string    `json:"metric_name"`
	Instance         string    `json:"instance"`
	ExportedInstance string    `json:"exported_instance"`
	HealthState      string    `json:"health_state"`
	Severity         string    `json:"severity"`
	ZScore           float64   `json:"z_score"`
	CompositeScore   float64   `json:"composite_score"`
	AnomalyPct       float64   `json:"anomaly_pct"`
	CurrentValue     float64   `json:"current_value"`
	BaselineMean     float64   `json:"baseline_mean"`
	BaselineStddev   float64   `json:"baseline_stddev"`
	TrendSlope       float64   `json:"trend_slope"`
	SpikeScore       float64   `json:"spike_score"`
	ConsecutiveCycles int      `json:"consecutive_cycles"`
	LastAnomalyTime  time.Time `json:"last_anomaly_time"`
	Silenced         bool      `json:"silenced"`
	BaseSamples      int       `json:"base_samples"`
	Confidence       float64   `json:"confidence"`
	WeekendBaseline  bool      `json:"weekend_baseline"`
	IsAnomaly        bool      `json:"is_anomaly"`
}

var (
	statusMu      sync.RWMutex
	currentStatus []StatusEntry
)

func setStatus(entries []StatusEntry) {
	statusMu.Lock()
	defer statusMu.Unlock()
	currentStatus = entries
}

func getStatus() []StatusEntry {
	statusMu.RLock()
	defer statusMu.RUnlock()
	out := make([]StatusEntry, len(currentStatus))
	copy(out, currentStatus)
	return out
}

// ─── Core Analysis ────────────────────────────────────────────────────────────

func analyzeQuery(cfg *Config, qc QueryConfig, loc *time.Location) []StatusEntry {
	// Resolve effective settings
	threshold := cfg.ZThreshold
	if qc.ZThreshold > 0 {
		threshold = qc.ZThreshold
	}
	minSamples := cfg.MinSamples
	if qc.MinSamples > 0 {
		minSamples = qc.MinSamples
	}
	minDelta := cfg.MinDelta
	if qc.MinDelta >= 0 {
		minDelta = qc.MinDelta
	}
	minRelative := cfg.MinRelative
	if qc.MinRelative >= 0 {
		minRelative = qc.MinRelative
	}

	// Handle silenced metrics
	if qc.Silenced {
		gSilenced.WithLabelValues(qc.Name).Set(1)
		return nil
	}
	gSilenced.WithLabelValues(qc.Name).Set(0)

	now := time.Now().In(loc)
	useWeekend := isWeekend(now)
	windowDur := time.Duration(cfg.WindowMinutes) * time.Minute
	step := cfg.StepSeconds

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Query current window
	curSamples, err := queryRange(ctx, cfg.PrometheusURL, qc.Query, now.Add(-windowDur), now, step)
	if err != nil {
		logf("ERROR", "query current %s: %v", qc.Name, err)
		cScrapeErrors.WithLabelValues(qc.Name).Inc()
		return nil
	}

	// Build current map: key -> values
	type seriesInfo struct {
		vals             []float64
		instance         string
		exportedInstance string
		lastVal          float64
	}
	curMap := make(map[string]*seriesInfo)
	for _, s := range curSamples {
		vals := parseValues(s.Values)
		if len(vals) == 0 {
			continue
		}
		inst := s.Metric["instance"]
		expInst := s.Metric["exported_instance"]
		if expInst == "" {
			expInst = s.Metric["exported_instance"]
		}
		key := seriesKey(qc.Name, inst, expInst)
		if _, ok := curMap[key]; !ok {
			curMap[key] = &seriesInfo{instance: inst, exportedInstance: expInst}
		}
		curMap[key].vals = append(curMap[key].vals, vals...)
		if len(vals) > 0 {
			curMap[key].lastVal = vals[len(vals)-1]
		}
	}

	// Build historical baseline: same time window on past N days, same weekday type
	histMap := make(map[string][]float64)
	for d := 1; d <= cfg.LookbackDays; d++ {
		pastDay := now.AddDate(0, 0, -d)
		if isWeekend(pastDay) != useWeekend {
			continue
		}
		ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
		hist, err := queryRange(ctx2, cfg.PrometheusURL, qc.Query,
			pastDay.Add(-windowDur), pastDay.Add(windowDur), step)
		cancel2()
		if err != nil {
			continue
		}
		for _, s := range hist {
			inst := s.Metric["instance"]
			expInst := s.Metric["exported_instance"]
			key := seriesKey(qc.Name, inst, expInst)
			histMap[key] = append(histMap[key], parseValues(s.Values)...)
		}
	}

	var entries []StatusEntry

	for key, cur := range curMap {
		hist := histMap[key]
		samples := len(hist)

		// Core labels
		lbls := prometheus.Labels{
			"metric_name":       qc.Name,
			"instance":          cur.instance,
			"exported_instance": cur.exportedInstance,
		}

		gHour.With(lbls).Set(float64(now.Hour()))
		gWeekend.With(lbls).Set(boolFloat(useWeekend))
		gSamples.With(lbls).Set(float64(samples))

		currentVal := cur.lastVal
		gCurrent.With(lbls).Set(currentVal)

		if samples < minSamples {
			logf("DEBUG", "skip %s (samples=%d < min=%d)", key, samples, minSamples)
			gDetected.With(lbls).Set(0)
			continue
		}

		mean, stddev := meanStddev(hist)
		gMean.With(lbls).Set(mean)
		gStddev.With(lbls).Set(stddev)

		confidence := confidenceFactor(samples)
		gConfidence.With(lbls).Set(confidence)

		var zScore float64
		if stddev > 0 {
			zScore = (currentVal - mean) / stddev
		}
		gZScore.With(lbls).Set(zScore)

		slope := linearSlope(cur.vals)
		var slopeNorm float64
		if stddev > 0 {
			slopeNorm = slope / stddev
		}
		spike := spikeScore(cur.vals, stddev)
		gSlope.With(lbls).Set(slope)
		gSlopeNorm.With(lbls).Set(slopeNorm)
		gSpike.With(lbls).Set(spike)

		composite := compositeScore(zScore, slopeNorm, spike, confidence)
		gComposite.With(lbls).Set(composite)

		// anomaly_percentage now measures current window (not historical)
		pct := anomalousPercent(cur.vals, mean, stddev, threshold)
		gPct.With(lbls).Set(pct)

		// ── False positive guards ──────────────────────────────────────────
		absDiff := math.Abs(currentVal - mean)

		// Guard 1: absolute minimum change
		if minDelta > 0 && absDiff < minDelta {
			gDetected.With(lbls).Set(0)
			setClassificationMetrics(lbls, "normal", "normal")
			continue
		}

		// Guard 2: relative minimum change (% of mean)
		if minRelative > 0 && mean > 0 && (absDiff/mean) < minRelative {
			gDetected.With(lbls).Set(0)
			setClassificationMetrics(lbls, "normal", "normal")
			continue
		}

		// Guard 3: stddev too small (near-zero metric) — require minimum stddev
		// If stddev < 1% of mean, the metric is essentially constant — skip
		if mean > 0 && stddev > 0 && (stddev/mean) < 0.01 {
			gDetected.With(lbls).Set(0)
			setClassificationMetrics(lbls, "normal", "normal")
			continue
		}

		isAnomaly := math.Abs(zScore) > threshold
		severity := severityLabel(composite)

		detected := 0.0
		if isAnomaly {
			detected = 1.0
		}
		gDetected.With(lbls).Set(detected)

		consecutive, healthState, lastAnomaly := globalState.Update(key, isAnomaly, severity)
		gConsecutive.With(lbls).Set(float64(consecutive))
		if !lastAnomaly.IsZero() {
			gLastAnomaly.With(lbls).Set(float64(lastAnomaly.Unix()))
		}

		setClassificationMetrics(lbls, severity, healthState)

		if isAnomaly {
			logf("ANOMALY", "%s inst=%s z=%+.2f comp=%.2f sev=%s state=%s consec=%d conf=%.2f",
				qc.Name, cur.instance, zScore, composite, severity, healthState, consecutive, confidence)
		}

		entries = append(entries, StatusEntry{
			MetricName: qc.Name, Instance: cur.instance,
			ExportedInstance: cur.exportedInstance,
			HealthState: healthState, Severity: severity,
			ZScore: zScore, CompositeScore: composite, AnomalyPct: pct,
			CurrentValue: currentVal, BaselineMean: mean, BaselineStddev: stddev,
			TrendSlope: slope, SpikeScore: spike,
			ConsecutiveCycles: consecutive, LastAnomalyTime: lastAnomaly,
			BaseSamples: samples, Confidence: confidence,
			WeekendBaseline: useWeekend, IsAnomaly: isAnomaly,
		})
	}
	return entries
}

func setClassificationMetrics(lbls prometheus.Labels, severity, healthState string) {
	for _, sev := range []string{"critical", "high", "medium", "low", "normal"} {
		v := 0.0
		if sev == severity {
			v = 1.0
		}
		gSeverity.With(prometheus.Labels{
			"metric_name": lbls["metric_name"], "instance": lbls["instance"],
			"exported_instance": lbls["exported_instance"], "severity_label": sev,
		}).Set(v)
	}
	for _, state := range []string{"normal", "warning", "recovering", "critical"} {
		v := 0.0
		if state == healthState {
			v = 1.0
		}
		gHealthState.With(prometheus.Labels{
			"metric_name": lbls["metric_name"], "instance": lbls["instance"],
			"exported_instance": lbls["exported_instance"], "state": state,
		}).Set(v)
	}
}

func boolFloat(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}

// ─── Main Loop ────────────────────────────────────────────────────────────────

func runLoop(configPath string) {
	for {
		start := time.Now()
		cfg, err := loadConfig(configPath)
		if err != nil {
			logf("ERROR", "load config: %v", err)
			time.Sleep(30 * time.Second)
			continue
		}

		loc, err := time.LoadLocation(cfg.Timezone)
		if err != nil {
			logf("WARN", "invalid timezone %q, using UTC: %v", cfg.Timezone, err)
			loc = time.UTC
		}

		now := time.Now().In(loc)
		logf("INFO", "cycle start — local=%s weekend=%v tz=%s",
			now.Format("15:04:05"), isWeekend(now), cfg.Timezone)

		var allEntries []StatusEntry
		var anomalyCount int

		for _, qc := range cfg.Queries {
			entries := analyzeQuery(cfg, qc, loc)
			allEntries = append(allEntries, entries...)
			for _, e := range entries {
				if e.IsAnomaly {
					anomalyCount++
				}
			}
		}

		// Sort by composite score
		sort.Slice(allEntries, func(i, j int) bool {
			return allEntries[i].CompositeScore > allEntries[j].CompositeScore
		})
		setStatus(allEntries)
		gLastScrape.SetToCurrentTime()

		elapsed := time.Since(start)
		logf("INFO", "cycle done %.1fs — %d/%d anomalous. next in %ds",
			elapsed.Seconds(), anomalyCount, len(allEntries), cfg.IntervalSeconds)

		time.Sleep(time.Duration(cfg.IntervalSeconds) * time.Second)
	}
}

// ─── HTTP Handlers ────────────────────────────────────────────────────────────

func statusHandler(w http.ResponseWriter, r *http.Request) {
	entries := getStatus()
	stateFilter := r.URL.Query().Get("state")
	sevFilter := r.URL.Query().Get("severity")
	metricFilter := r.URL.Query().Get("metric")

	var filtered []StatusEntry
	for _, e := range entries {
		if stateFilter != "" && e.HealthState != stateFilter {
			continue
		}
		if sevFilter != "" && e.Severity != sevFilter {
			continue
		}
		if metricFilter != "" && !strings.Contains(e.MetricName, metricFilter) {
			continue
		}
		filtered = append(filtered, e)
	}

	bySev := map[string]int{}
	byState := map[string]int{}
	totalAnom := 0
	for _, e := range filtered {
		bySev[e.Severity]++
		byState[e.HealthState]++
		if e.IsAnomaly {
			totalAnom++
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"generated_at":    time.Now().UTC().Format(time.RFC3339),
		"total_series":    len(filtered),
		"total_anomalies": totalAnom,
		"by_severity":     bySev,
		"by_health_state": byState,
		"series":          filtered,
	})
}

func configHandler(w http.ResponseWriter, r *http.Request) {
	cfg, err := loadConfig(getConfigPath())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cfg)
}

func reloadHandler(w http.ResponseWriter, r *http.Request) {
	cfg, err := loadConfig(getConfigPath())
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), 400)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok", "queries": len(cfg.Queries),
	})
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"status":"ok"}`)
}

var configPathGlobal string

func getConfigPath() string { return configPathGlobal }

func logf(level, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[%s] %s", level, msg)
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	configPath := "config.ini"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	configPathGlobal = configPath

	// Allow TZ override from environment (before config load)
	if tz := os.Getenv("TZ"); tz != "" {
		if loc, err := time.LoadLocation(tz); err == nil {
			_ = loc
			logf("INFO", "using TZ from environment: %s", tz)
		}
	}

	go runLoop(configPath)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/status", statusHandler)
	mux.HandleFunc("/config", configHandler)
	mux.HandleFunc("/reload", reloadHandler)
	mux.HandleFunc("/healthz", healthHandler)

	// Load config just to get listen addr
	cfg, err := loadConfig(configPath)
	if err != nil {
		logf("WARN", "using default listen addr: %v", err)
		cfg = defaultConfig()
	}

	logf("INFO", "listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
