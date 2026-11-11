"use strict";

const candyExpanded = new Set();
const candyAnswersExpanded = new Set();
const candySelected = new Set();

const candyRuns = () => boundedInput("candy-runs", 1, 10);

const candyScopeMatch = (r) => r.model === $("candy-model").value && (r.effort || "none") === $("candy-effort").value;
const CANDY_STATUS = { correct: ["ok", "答对"], wrong: ["bad", "答错"], failed: ["err", "出错"], skipped: ["skip", "已跳过"] };
const candyKind = (r) => (CANDY_STATUS[r.status] || CANDY_STATUS.failed)[0];
const candyVerdict = (r) => statusPill(candyKind(r), (CANDY_STATUS[r.status] || CANDY_STATUS.failed)[1]);
const candyRecordMeta = (r) => `<div class="result-meta">${candyVerdict(r)}${metric("calendar", "测试时间", fmtTime(r.time))}${modelMeta(r)}</div>`;
// Answers come from untrusted upstreams: raw HTML is escaped, links and images stay text, and Temml
// refuses commands that need trust, such as \href. The CJK plugin lets emphasis close after full-width
// punctuation, as in **答案：**21.
const markdown = markdownit({ breaks: true }).disable(["link", "image", "autolink"])
  .use(markdownItCjkFriendly).use(texmath, { engine: temml, delimiters: ["dollars", "brackets"] });
const answerHTML = (r) => r.error ? esc(r.error) : markdown.render(r.answer || "");
const answerKey = (credentialID, r) => JSON.stringify([credentialID, r.id]);
const answerToggle = (open) => `${icon("chevron-right", "chev")}${open ? "收起" : "展开"}`;

function candyHistory(a, r) {
  const key = answerKey(a.id, r);
  const answerOpen = candyAnswersExpanded.has(key);
  const bodyID = `answer-${encodeURIComponent(a.id)}-${encodeURIComponent(r.id)}`;
  return historyCard(r, `<div class="answer ${answerOpen ? "expanded" : ""}">${candyVerdict(r)}
    <button class="answer-toggle" type="button" data-answer="${esc(key)}" aria-expanded="${answerOpen}" aria-controls="${esc(bodyID)}" aria-label="${answerOpen ? "收起文本" : "展开文本"}">${answerToggle(answerOpen)}</button>
    <div id="${esc(bodyID)}" class="answer-content ${r.error ? "error" : "markdown"}">${answerHTML(r)}</div>
  </div>`);
}

function renderCandyRow(a) {
  const records = a.candies, last = records.at(-1);
  const graded = records.filter((r) => candyScopeMatch(r) && (r.status === "correct" || r.status === "wrong"));
  const correct = graded.filter((r) => r.status === "correct").length;

  const rate = graded.length
    ? `<b class="mono">${Math.round((correct / graded.length) * 100)}%</b><small class="mono">${correct}/${graded.length}</small>`
    : `<span class="none">—</span>`;
  const marks = records.map((r) => `<button class="mark-hit" type="button" data-credential="${esc(a.id)}" data-record="${esc(r.id)}" aria-label="${esc(`${fmtTime(r.time)} ${modelName(r)} ${CANDY_STATUS[r.status]?.[1] || ""}`)}">${icon(STATUS_ICONS[candyKind(r)], `mark ${candyKind(r)} ${candyScopeMatch(r) ? "" : "dim"}`)}</button>`).join("");
  const progress = a.running?.test === "candy" && a.running;
  const blocker = runBlocker("candy", a);
  const button = progress
    ? collectionButton("candy", a, progress, "测试")
    : `<button class="btn" type="button" data-candy-run="${esc(a.id)}" ${disabledFor(blocker)}>${icon("play")}测试</button>`;

  return credentialRow({
    test: "candy", credential: a, selected: candySelected.has(a.id), open: candyExpanded.has(a.id), selectable: !blocker, button,
    result: progress ? collectionOutcome(progress) : latestResult(last, candyVerdict), meta: `<div class="rate">${rate}</div><div class="marks">${marks}</div>`, tested: records.length > 0,
    history: () => [...records].reverse().map((r) => candyHistory(a, r)).join(""),
  });
}

function runCandy(body) {
  if (!$("candy-toolbar").reportValidity()) return;
  return startTest("candy", { ...body, model: $("candy-model").value, effort: $("candy-effort").value, runs: candyRuns() });
}

function candyTip(target) {
  const r = credentials.find((a) => a.id === target.dataset.credential)?.candies.find((r) => r.id === target.dataset.record);
  return r ? `${candyRecordMeta(r)}
    <div class="metrics">${r.status === "skipped" ? "" : metrics(r)}</div>
    <div class="tip-text ${r.error ? "error" : "markdown"}">${answerHTML(r)}</div>` : "";
}

function renderCandy() {
  $("candy-rate-scope").textContent = `正确率按 ${modelName({ model: $("candy-model").value, effort: $("candy-effort").value })} 统计`;
  renderSelection("candy", candySelected, "测试");
  $("candy-clear").disabled = pending || credentials.every((a) => !a.candies.length);
  renderList("candy", renderCandyRow);
}

function initializeCandy() {
  fillSelect("candy-effort", DEFAULT_EFFORTS, preference("candy-effort", DEFAULT_EFFORT), DEFAULT_EFFORT);
  $("candy-runs").value = preference("candy-runs", 1);
  $("candy-runs").value = candyRuns();

  for (const id of ["candy-model", "candy-effort"]) $(id).addEventListener("change", render);
  $("candy-runs").addEventListener("change", () => { $("candy-runs").value = candyRuns(); });
  $("candy-rows").addEventListener("click", (e) => {
    if (e.target.closest("label")) return;
    const hit = e.target.closest("[data-record]");
    if (hit) return showTip(hit);
    const toggle = e.target.closest("[data-answer]");
    if (toggle) {
      const key = toggle.dataset.answer;
      const open = !candyAnswersExpanded.has(key);
      open ? candyAnswersExpanded.add(key) : candyAnswersExpanded.delete(key);
      toggle.closest(".answer").classList.toggle("expanded", open);
      toggle.setAttribute("aria-expanded", open);
      toggle.setAttribute("aria-label", open ? "收起文本" : "展开文本");
      toggle.innerHTML = answerToggle(open);
      return;
    }
    const btn = e.target.closest("[data-candy-run]");
    if (btn) return runCandy({ credential_ids: [btn.dataset.candyRun] });
    const cancel = e.target.closest("[data-candy-cancel]");
    if (cancel) return update("停止测试失败", () => api(`${BASE}/cancel`, { method: "POST", body: { credential_ids: [cancel.dataset.candyCancel] } }));
    const row = e.target.closest("[data-row]");
    if (!row) return;
    const id = row.dataset.row;
    candyExpanded.has(id) ? candyExpanded.delete(id) : candyExpanded.add(id);
    render();
  });
}
