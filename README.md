# fde-07-durable-hitl

**Forward Deployed Engineering — Stage 7 of 12: Human-in-the-Loop & Durable Workflows**

Agent workflows that survive crashes, wait days for a human, never refund twice, and leave a record legal can trust.

**Also built as Project 7 of 12, "Async Temporal Approval Gate",** from the companion [12 FDE projects post](https://x.com/suraj_sharma14/status/2125197762752806998).

**Live demo:** https://fde-07-durable-hitl.braylo777.workers.dev — the Rust core runs in your browser as WebAssembly. No server, no tracking.

## Why this matters

An agent that can issue a $900 refund needs a manager to approve it, and the approval may come hours later, after a deploy or a crash. If the process restarts and runs the refund again, that is real money. This repo shows the pattern that Temporal and similar engines use: the event history is the checkpoint, every step is replayed from it, side effects are keyed so they happen once, and every human decision is chained into an audit trail.

## What it does

- Event-sourced engine: state is a fold over history; each loop pass writes one event. A crash after any persisted event resumes to the same event sequence and final state (tested at every crash point)
- Tasks with retries and exponential backoff; final failure runs saga compensations in reverse
- Approval gates: conditional (amount over a limit), quorum, role check, separation of duties, duplicate-vote guard, timeout, one escalation
- Idempotency keys per workflow run (run_id or a hash of the start input): a task re-run after a crash returns its earlier result
- Slack Block Kit and Teams Adaptive Card approval messages; Slack request-signature check with a 5-minute replay window
- Hash-chained accountability trail of votes, refusals, escalations and side effects

## What it does not do (yet)

- Not Temporal itself: no worker fleet, task queues or persistence layer; history is returned to the caller to store
- Timers are virtual (signal timestamps), not wall-clock
- No live Slack or Teams app: the message JSON and the signature check are real, the posting is not
- The crash test stops after an event is saved; the 'effect ran but its event was lost' window relies on the side-effect store honouring the idempotency key

## Operations

Every operation takes JSON in and gives JSON out. The CLI, the tests and the demo page all use the same entry point.

| Operation | What it does |
|---|---|
| `run` | A $900 refund passes lookup and draft, then stops at the manager gate. status is waiting: store history and resume when someone clicks. |
| `crash_test` | Run once cleanly, run again and kill it after 9 events (mid-task), then resume from the saved history. Histories match and the refund happened exactly once. Try any crash_after_events value. |
| `approval_message` | Block Kit JSON for the approval request. Set channel to teams for an Adaptive Card. |
| `verify_slack_signature` | Checks the X-Slack-Signature header before trusting a button click. Change one character of the body, or move now forward 10 minutes, to see it fail. |

## Run it

You need Rust (stable). No Node, no npm.

```bash
git clone https://github.com/braylo777/fde-07-durable-hitl
cd fde-07-durable-hitl
cargo test
cargo run -- ops
```

Try the first demo example from the command line:

```bash
python3 -c 'import json;print(json.dumps(json.load(open("web/examples.json"))["ops"][0]["input"]))' | cargo run -q -- run
```

## Build and deploy the demo

```bash
./build.sh    # tests, then compiles the core to web/app.wasm
./deploy.sh   # serves web/ as a Cloudflare Workers static-assets site (needs a Cloudflare login)
```

`web/` is a plain static site (`index.html`, `app.js`, `app.wasm`). It has no build step, so any static host works.

## Layout

```
src/lib.rs        operation list and router
src/api.rs        JSON entry point + the 3 WASM exports (alloc, dealloc, run)
src/main.rs       CLI
src/*.rs          one module per capability, each with unit tests
tests/            integration tests (every demo example must pass)
web/              demo page; examples.json drives both the page and this README
```

## Design choices

- **Rust, few dependencies.** Only `serde`, `serde_json`, `sha2` and `hmac`, pinned to exact versions with a committed `Cargo.lock`.
- **Deterministic.** Time, randomness and keys are inputs, so every result can be replayed in a test or an audit.
- **Fail closed.** Unknown input, missing fields or an unmatched policy gives an error or a denial, never a silent pass.

## Part of a series

This is one of 12 repos that work through a public Forward Deployed Engineering roadmap ([12 stages](https://x.com/suraj_sharma14/status/2124827808535048566), [12 projects](https://x.com/suraj_sharma14/status/2125197762752806998)). All 12: [github.com/braylo777?q=fde-](https://github.com/braylo777?tab=repositories&q=fde-).

## License

MIT — see [LICENSE](LICENSE). Built by [Braylo SI](https://braylo.si).
