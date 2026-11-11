package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
)

// progress is the test a credential is running; a credential runs one test at a time.
type progress struct {
	Test       string `json:"test"`
	Model      string `json:"model"`
	Done       int    `json:"done"`
	Total      int    `json:"total"`
	Queued     bool   `json:"queued,omitempty"`
	Cancelling bool   `json:"cancelling,omitempty"`
	cancel     context.CancelFunc
}

// running maps credential IDs to the test they run, guarded by mu.
var running = map[string]*progress{}

const (
	defaultCredentialConcurrency = 2
	maxCredentialConcurrency     = 10
)

// The queue and active count are shared by every test and management request, guarded by mu.
type credentialRun struct {
	auth credential
	p    *progress
	ctx  context.Context
	run  func(context.Context, credential, *progress)
}

var pendingRuns []*credentialRun
var activeRuns int

type schedulerStatus struct {
	CredentialConcurrency int `json:"credential_concurrency"`
	Active                int `json:"active"`
	Queued                int `json:"queued"`
}

func schedulerStatusLocked() schedulerStatus {
	return schedulerStatus{records.CredentialConcurrency, activeRuns, len(pendingRuns)}
}

func settingsResponse(body []byte) managementResponse {
	var req struct {
		CredentialConcurrency int `json:"credential_concurrency"`
	}
	if json.Unmarshal(body, &req) != nil || req.CredentialConcurrency < 1 || req.CredentialConcurrency > maxCredentialConcurrency {
		return jsonError(http.StatusBadRequest, fmt.Sprintf("凭证并发须为 1–%d 的整数", maxCredentialConcurrency))
	}
	mu.Lock()
	defer mu.Unlock()
	if runCtx.Err() != nil {
		return jsonError(http.StatusServiceUnavailable, "插件正在停止，请稍后重试")
	}
	previous := records.CredentialConcurrency
	records.CredentialConcurrency = req.CredentialConcurrency
	if err := saveStateLocked(); err != nil {
		records.CredentialConcurrency = previous
		return jsonError(http.StatusInternalServerError, "保存并发设置失败："+err.Error())
	}
	dispatchRunsLocked()
	return jsonResponse(http.StatusOK, schedulerStatusLocked())
}

// Dispatch in submission order. A credential holds its slot through all its steps and retries.
// Decreasing the limit lets active work finish; no new work starts until it falls below the limit.
func dispatchRunsLocked() {
	for runCtx.Err() == nil && activeRuns < records.CredentialConcurrency && len(pendingRuns) > 0 {
		job := pendingRuns[0]
		pendingRuns[0] = nil
		pendingRuns = pendingRuns[1:]
		job.p.Queued = false
		activeRuns++
		go func() {
			defer tasks.Done()
			defer func() {
				job.p.cancel()
				mu.Lock()
				delete(running, job.auth.ID)
				activeRuns--
				dispatchRunsLocked()
				mu.Unlock()
			}()
			job.run(job.ctx, job.auth, job.p)
		}()
	}
}

// Cancel queued work without starting it or creating a test record. Caller holds mu.
func discardCancelledRunsLocked() {
	kept := pendingRuns[:0]
	for _, job := range pendingRuns {
		if job.ctx.Err() != nil {
			job.p.cancel()
			delete(running, job.auth.ID)
			tasks.Done()
		} else {
			kept = append(kept, job)
		}
	}
	clear(pendingRuns[len(kept):])
	pendingRuns = kept
}

// runRequest selects the credentials a test runs on and its model. ModelCatalog lists the models of the
// credentials whose model list the page could read.
type runRequest struct {
	CredentialIDs []string            `json:"credential_ids"`
	All           bool                `json:"all"`
	Model         string              `json:"model"`
	ModelCatalog  map[string][]string `json:"model_catalog,omitempty"`
}

type runSummary struct {
	Started   int `json:"started"`
	Busy      int `json:"busy,omitempty"`
	Skipped   int `json:"skipped,omitempty"`
	Unchecked int `json:"unchecked,omitempty"`
	Queued    int `json:"queued,omitempty"`
}

// startRuns starts test, which takes total steps, on each selected credential that is idle. Credentials
// whose model list lacks the model get skip instead, which runs with mu held. run enters the shared queue,
// with a context that ends when cancelled or the plugin quiesces.
func startRuns(test string, req runRequest, total int, skip func(id string), run func(ctx context.Context, auth credential, p *progress)) managementResponse {
	auths, err := selectedCredentials(req.CredentialIDs, req.All)
	if err != nil {
		return jsonError(http.StatusBadGateway, err.Error())
	}
	return startCredentialRuns(test, req, auths, total, skip, run)
}

func startCredentialRuns(test string, req runRequest, auths []credential, total int, skip func(string), run func(context.Context, credential, *progress)) managementResponse {
	if len(auths) == 0 {
		return jsonError(http.StatusBadRequest, "没有可测试的已启用凭证")
	}
	mu.Lock()
	defer mu.Unlock()
	if runCtx.Err() != nil {
		return jsonError(http.StatusServiceUnavailable, "插件正在停止，请稍后重试")
	}
	summary := runSummary{}
	var admitted []*progress
	for _, auth := range auths {
		if running[auth.ID] != nil {
			summary.Busy++
			continue
		}
		if known, supported := credentialSupportsModel(req.ModelCatalog, auth.ID, req.Model); known && !supported {
			skip(auth.ID)
			summary.Skipped++
			continue
		} else if !known {
			summary.Unchecked++
		}
		summary.Started++
		ctx, cancel := context.WithCancel(runCtx)
		p := &progress{Test: test, Model: req.Model, Total: total, Queued: true, cancel: cancel}
		running[auth.ID] = p
		admitted = append(admitted, p)
		tasks.Add(1)
		pendingRuns = append(pendingRuns, &credentialRun{auth: auth, p: p, ctx: ctx, run: run})
	}
	dispatchRunsLocked()
	for _, p := range admitted {
		if p.Queued {
			summary.Queued++
		}
	}
	if summary.Skipped > 0 {
		_ = saveStateLocked()
	}
	return jsonResponse(http.StatusOK, summary)
}

// cancelResponse stops the tests running on the credentials in body: they send no new requests and abandon
// those in flight.
func cancelResponse(body []byte) managementResponse {
	var req struct {
		CredentialIDs []string `json:"credential_ids"`
	}
	if json.Unmarshal(body, &req) != nil {
		return jsonError(http.StatusBadRequest, "请求格式错误")
	}
	mu.Lock()
	defer mu.Unlock()
	count := 0
	for id, p := range running {
		if slices.Contains(req.CredentialIDs, id) {
			p.Cancelling = true
			p.cancel()
			count++
		}
	}
	discardCancelledRunsLocked()
	return jsonResponse(http.StatusOK, map[string]int{"cancelled": count})
}
