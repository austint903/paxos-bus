*This project is still in progress.*

# PaxosBus: Geo-Replicated State Machines

This repository contains the prototype implementation of PaxosBus, a state-machine replication protocol for geo-distributed systems. PaxosBus explores how shared schedules can help replicas agree on the order of commands with less coordination. Our approach groups client requests into batches, or “buses,” and uses an agreed-upon schedule to organize their execution across replicas.

PaxosBus has been accepted to SOSP SysDW '26.
