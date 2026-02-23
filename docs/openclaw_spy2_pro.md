# OpenClaw `spy2-pro` Agent

## Purpose
- Autonomous engineering/research agent for SPY_2 only.
- Constrained to paper-trading workflows on IBKR.

## Hard Constraints
- Filesystem scope: sandboxed workspace only (`/home/karson/SPY_2` mounted as `/workspace`).
- No cross-project access.
- No live trading ports (`4001`, `7496`).
- Use IBKR paper endpoints only (`4002` Gateway / `7497` TWS).

## Runtime
- Agent id: `spy2-pro`
- Model alias: `gpt-5.3-codex-xhigh` -> `openai-codex/gpt-5.3-codex`
- Thinking: `high` (highest level supported by current OpenClaw CLI)
- Web research: Brave-backed `web_search` enabled from global OpenClaw config.
- Sandbox image: `openclaw-sandbox-spy2:bookworm-slim`

## Quick Start
```bash
scripts/spy2_openclaw_agent.sh "Run IBKR paper connectivity check and summarize."
```

## Direct Invocation
```bash
openclaw agent --agent spy2-pro --thinking high --message "<task>"
```

## Sandbox Maintenance
```bash
openclaw sandbox explain --agent spy2-pro
openclaw sandbox recreate --agent spy2-pro --force
```

## Notes
- If a fresh sandbox needs dependency bootstrap, run one warm-up task first.
- For IBKR checks from inside sandbox, prefer:
  - `uv run python -m spy2 ibkr check --gateway --host host.docker.internal --confirm-read-only-unchecked`
