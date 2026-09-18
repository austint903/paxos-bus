# Client pause and acknowledged resume

Clients pause request generation, bus production, and timeout retries when a
replica announces a newer view. A dedicated TCP control connection to each
replica keeps this notification separate from queued bus data and request
replies. Periodic status queries recover missed notifications; failed control
connections reconnect independently of the data connections.

Already-issued buses continue draining. Each client first waits for a quorum
including the new leader to report `Normal` in the same view. It then proposes
its next bus sequence and arrival time. A replica acknowledges only when it is
normal in that exact view and has received the client's previously issued bus
sequence. The client resumes after a leader-containing quorum acknowledges the
same proposal. A newer view invalidates the handshake; stale tokens, duplicate
replica votes, and acknowledgments for another schedule do not count.

Resume changes **arrival predictions only**. The original ordering lines and
bus-to-slot mapping remain immutable, including for retained and in-flight
buses. Gap detection uses the adjusted arrival prediction. Registered clients
that have not resumed receive a bounded grace period (one configured view-change
fallback interval); a silent client cannot suppress gap repair indefinitely.

The generator and bus clock restart without producing a catch-up burst for the
paused duration. Previously generated requests retain their latency origin,
and uncommitted requests retain the normal retry behavior. Pausing does not
discard sender queues or already-issued requests. It cannot instantly remove
data already buffered in TCP or at a replica.

## Running

Build/deploy both client and replica binaries from this branch. Existing launch
commands enable the feature automatically. Client flags:

- `-pause-on-view-change=true` (default); set `false` for a paired baseline.
- `-recovery-wait-ms=1000` (default): minimum future departure scheduling lead.
  The client increases this if needed for the measured WAN delay. This is not a
  substitute for acknowledgments: an unacknowledged/expired proposal is retried.

Look for `CLIENT-PAUSE view=...` and
`CLIENT-RESUME view=... next_bus=... schedule_acked=true` in client logs.
The existing `VIEW-CHANGE done` lines report replica recovery separately.

The current `gcp-scripts/run-gcp.sh` builds the working tree and needs no new
flags to enable pausing. Its existing `--resend-ms` still controls request
retries, not handshake or view-change timeouts.

## Measurement and validation

This benchmark pauses **generation**, so offered load falls to zero during the
pause. Plot pause duration, first resumed commit, and sustained throughput
separately; do not describe the paused interval as continuously offered 100k or
160k. In an application that continues accepting requests upstream, preserve
their original arrival times when measuring latency.

Run `go test -race ./paxosbus ./paxosbus/cmd/...`. Regression coverage includes
unchanged slot mappings, old-data draining, stale/conflicting schedules,
leader-containing quorums, view changes during the handshake, sparse-fetch
progress, and cancellation of obsolete fetches on still-connected peers.

The recovery changes also remove the 100ms backoff after a missing run fills
and recheck fetch view/generation while awaiting a response. Empty/failed
responses retain retry backoff, and normal/view-change publication fences
remain in place.
