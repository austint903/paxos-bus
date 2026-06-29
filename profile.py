"""PaxosBus single-node CloudLab profile (wide-area experiment).

Import this file's *source* directly when creating each experiment
(CloudLab portal -> "Create Experiment Profile" -> upload/paste this file).
Instantiate it ONCE PER CLUSTER (Utah, Wisconsin, Clemson, Mass); pick the
cluster from the portal's cluster drop-down at instantiate time -- this profile
is cluster-agnostic, so the same source is reused for all four experiments.

On boot the node clones github.com/austint903/paxos-bus and runs
cloudlab/setup.sh, which installs Go + build deps and builds the PaxosBus
binaries, so the machine is ready the moment you can SSH in. The bootstrap is
self-contained on purpose: it does NOT rely on CloudLab cloning the repo to
/local/repository (that only happens for repository-based profiles, not for a
directly-imported one). See cloudlab/README.md for the full runbook.
"""

import geni.portal as portal
import geni.rspec.pg as pg

pc = portal.Context()
request = pc.makeRequestRSpec()

pc.defineParameter(
    "hw_type",
    "Hardware type (leave blank to take any free node at the chosen cluster)",
    portal.ParameterType.NODETYPE, "")
pc.defineParameter(
    "repo_url", "Git repo cloned and built on boot",
    portal.ParameterType.STRING,
    "https://github.com/austint903/paxos-bus.git")
params = pc.bindParameters()

node = request.RawPC("node")
node.disk_image = \
    "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD"
if params.hw_type:
    node.hardware_type = params.hw_type

# Public, internet-routable control IP so nodes in the OTHER experiments (each a
# separate experiment in a separate cluster) can reach this one over the control
# network -- the only cross-experiment path, since experiment networks are
# VLAN-isolated per experiment.
node.routable_control_ip = True

# Self-bootstrapping startup service: clone the repo and run setup.sh. Runs on
# every boot once the node is up; all output goes to /local/setup.log on the
# node. Kept as a single inline command so this profile.py is fully standalone.
bootstrap = (
    "sudo bash -c '"
    # Redirect INSIDE the root shell: an Execute service runs as the
    # unprivileged geniuser, and /local needs root to write, so the log file
    # must be opened by root (sudo), not by the outer user shell.
    "exec > /local/setup.log 2>&1; "
    "export DEBIAN_FRONTEND=noninteractive; "
    "apt-get update -y && apt-get install -y git; "
    "if [ ! -d /local/paxos-bus/.git ]; then "
    "git clone " + params.repo_url + " /local/paxos-bus; "
    "else git -C /local/paxos-bus pull --ff-only || true; fi; "
    "REPO_URL=" + params.repo_url + " bash /local/paxos-bus/cloudlab/setup.sh"
    "'"
)
node.addService(pg.Execute(shell="bash", command=bootstrap))

pc.printRequestRSpec(request)
