# ByzRaft: RAFT–PBFT Hybrid Consensus

A hybrid consensus system that runs **Raft** during normal operation and
**dynamically switches to PBFT** when Byzantine (malicious) behavior is
detected — avoiding PBFT's quadratic message overhead during fault-free
operation while still tolerating malicious nodes when it matters.

> **Built on [Zatt](https://github.com/simonacca/zatt)**, an open-source Raft
> implementation by Simone Accascina (University of Trento). Zatt provides the
> Raft consensus and replicated key–value store; **this project adds the
> Byzantine-fault-tolerance layer and the dynamic Raft↔PBFT switching described
> below.

## Motivation
Raft tolerates crash faults efficiently but assumes non-Byzantine nodes. PBFT
tolerates Byzantine faults but pays an O(n²) messaging cost every round. This
project gets the best of both: run cheap Raft while the cluster is healthy, and
escalate to PBFT only once malicious behavior is detected.

## What we added on top of Zatt
- **Byzantine detection:** [CONFIRM: 1–2 lines on how you detect malicious nodes]
- **Dynamic protocol switch:** transitions the cluster from Raft to PBFT (and
  back to Raft once system is not susceptible to Byzantine fault
- **PBFT round implementation**
- **Fault-injection testing**
