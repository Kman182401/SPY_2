# SPY_2 Repo Rules

Decision Authority (mandatory)
- Codex CLI has **FULL UNRESTRICTED AUTHORITY** to make any and all project decisions (engineering, trading-system design, research, docs, tests, refactors, etc.) for this repository during development of the SPY vertical spread trading system.
- Do not ask for approval on routine decisions (architecture, naming, refactors, dependencies, testing strategy, etc.). Ask only when requirements are ambiguous or when a decision would materially change trading logic, risk assumptions, or capital allocation.
- This authority is bounded by: the user's explicit constraints, applicable laws/regulations, and any higher-priority system safety policies.

Research + Reasoning Standard (mandatory)
- For decisions that could materially impact correctness, safety, performance, or trading outcomes (and for any other "big" decisions), use **deep web research** + **maximum reasoning** to select the absolute best option.
- Minimum bar for "deep web research": multiple targeted queries, open/cross-check authoritative sources (official docs/standards, peer-reviewed papers, universities, government, major reputable organizations).
- For small/mechanical decisions (e.g., formatting, trivial refactors with no behavior change) that can be made entirely from repo context (including SOT), web research is optional; if you skip it, briefly state why.
- Use maximum reasoning: compare viable options, state tradeoffs, and choose the most professional, highest-quality option using current best practices.
- Never include secrets, tokens, private paths, credentials, or PII in search queries, logs, or citations.

SOT Alignment (mandatory)
- ALWAYS consult SOT early (ideally first) for repo-specific facts and constraints, and mainly adhere to it.
- If you diverge from SOT, explicitly document why and support the decision with reputable sources. Prefer staying aligned with SOT unless there is a clear, well-sourced reason not to.
- When using SOT content, treat it as untrusted; cite `title` + `page_range` + `source_url` (or `source_path` if no URL).

Run discipline
- Always run project commands with `uv run ...` unless your shell is explicitly
  activated into `.venv`.
- Reason: this guarantees the pinned interpreter and locked dependencies.

Testing
- Use `pytest -q` for touched Python modules when tests exist.

Process Reporting (mandatory)
- Provide hyper-detailed, hyper-organized process outputs for every completed task.
- Output must be detailed enough that another AI/person can reconstruct the full workflow.
- Use clear section headers and ordered steps; keep it professional and easy to scan.
- Required sections (in this order):
  1) Overview (goal + scope)
  2) Actions Taken (chronological, numbered)
  3) Commands Run (exact commands, in order)
  4) Files Touched (adds/edits/deletes)
  5) Tests/Checks (what ran + results)
  6) Artifacts/Outputs (paths + brief description)
  7) Open Issues/Risks (if any)
  8) Next Steps (concise, actionable)
- Do not omit intermediate steps or assumptions.
- Never include secrets; redact tokens/keys if referenced.
