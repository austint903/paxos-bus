This guide provides step-by-step instructions for running PaxosBus experiments using the scripts in [`paxosbus/scripts`](../scripts/).

## Prerequisites

These scripts require prior authentication to Google Cloud Platform (GCP), a configured GCP project, and existing VMs in that project. Both the single-machine and multi-machine scripts run experiments on GCP VMs.

Create a `.env` file in the **repository root** and fill it out using [`.env.example`](../../.env.example) as a reference. Replace the placeholder project ID, VM names, and zones with your own values. The order of entries in `GCP_REPLICAS` and `GCP_CLIENTS` determines their IDs; both start at 0. Replica 0 is the initial leader.

For the majority of our PaxosBus experiments, we used the three-replica, eight-client regional setup described in [`.env.example.paxosbus`](../../.env.example.paxosbus).

Adjust `PAXOSBUS_PORT` (default: `7000`) and `GCP_GOARCH` (default: `amd64`; use `arm64` for ARM VMs) if needed.

<details>
<summary>Single-machine experiments</summary>

For single-VM smoke tests using `run-paxosbus-single-machine.sh`, configure your `.env` as follows:

- **Required:** `GCP_PROJECT_ID`, `GCP_SINGLE_VM`, and `GCP_SINGLE_ZONE`
- **Optional:** `PAXOSBUS_REPLICA_COUNT`, `PAXOSBUS_CLIENT_COUNT`, `PAXOSBUS_PORT`,  `GCP_GOARCH`
- **Not needed:** `GCP_REPLICAS`, `GCP_CLIENTS`

<details>
<summary>Default values if omitted</summary>

| Setting | Default |
| --- | --- |
| `PAXOSBUS_REPLICA_COUNT` | `3` replica processes |
| `PAXOSBUS_CLIENT_COUNT` | `8` client processes |
| `PAXOSBUS_PORT` | `7000` |
| `GCP_GOARCH` | `amd64` |

All replica and client processes run on the single configured VM.

</details>

<br>

Replace any placeholders in settings you keep with actual values. Omit optional settings to use their defaults

</details>

## Single-Machine Tests

## Multi-Machine Tests
