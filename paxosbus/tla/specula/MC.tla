------------------------------- MODULE MC -------------------------------
\* Counter-bounded exhaustive model for the six brief scenarios.

EXTENDS base
B == INSTANCE base

CONSTANTS BusLimit, SyncLimit, LossLimit, ConnectionLimit, ViewChangeLimit,
          DonorFailureLimit, PruneLimit, ReboardLimit,
          RegistrationLimit, MaxMsgBufferLimit

ASSUME /\ BusLimit \in Nat /\ SyncLimit \in Nat /\ LossLimit \in Nat
       /\ ConnectionLimit \in Nat /\ ViewChangeLimit \in Nat
       /\ DonorFailureLimit \in Nat
       /\ PruneLimit \in Nat /\ ReboardLimit \in Nat
       /\ RegistrationLimit \in Nat /\ MaxMsgBufferLimit \in Nat

VARIABLE constraintCounters
faultVars == <<constraintCounters>>
mcVars == <<vars, faultVars>>

\* Bounded environment/client introductions; reactive handlers remain unbounded.
MCClientAnnouncesSchedule(c, r) ==
    /\ constraintCounters.registration < RegistrationLimit
    /\ B!ClientAnnouncesSchedule(c, r)
    /\ constraintCounters' = [constraintCounters EXCEPT !.registration = @ + 1]

MCClientSendsBus(c, v, r) ==
    /\ constraintCounters.bus < BusLimit
    /\ B!ClientSendsBus(c, v, r)
    /\ constraintCounters' = [constraintCounters EXCEPT !.bus = @ + 1]

MCSendSyncPrepare(r, q, s) ==
    /\ constraintCounters.sync < SyncLimit
    /\ B!SendSyncPrepare(r, q, s)
    /\ constraintCounters' = [constraintCounters EXCEPT !.sync = @ + 1]

MCDisconnect(a, b) ==
    /\ constraintCounters.connection < ConnectionLimit
    /\ B!Disconnect(a, b)
    /\ constraintCounters' = [constraintCounters EXCEPT !.connection = @ + 1]

MCLoseMessage(m) ==
    /\ constraintCounters.loss < LossLimit
    /\ B!LoseMessage(m)
    /\ constraintCounters' = [constraintCounters EXCEPT !.loss = @ + 1]

MCBeginViewChange(r) ==
    /\ constraintCounters.viewChange < ViewChangeLimit
    /\ B!BeginViewChange(r)
    /\ constraintCounters' = [constraintCounters EXCEPT !.viewChange = @ + 1]

MCSetDonorUnavailable(r) ==
    /\ constraintCounters.donorFailure < DonorFailureLimit
    /\ donorAvailable[r]
    /\ B!SetDonorAvailable(r, FALSE)
    /\ constraintCounters' = [constraintCounters EXCEPT !.donorFailure = @ + 1]

MCPruneCommittedSlot(r, s) ==
    /\ constraintCounters.prune < PruneLimit
    /\ B!PruneCommittedSlot(r, s)
    /\ constraintCounters' = [constraintCounters EXCEPT !.prune = @ + 1]

MCClientReboards(v) ==
    /\ constraintCounters.reboard < ReboardLimit
    /\ B!ClientReboards(v)
    /\ constraintCounters' = [constraintCounters EXCEPT !.reboard = @ + 1]

MCInit ==
    /\ Init
    /\ constraintCounters = [registration |-> 0, bus |-> 0, sync |-> 0,
         loss |-> 0, connection |-> 0, viewChange |-> 0,
         donorFailure |-> 0, prune |-> 0, reboard |-> 0]

ReactiveNext ==
    \/ \E r \in Replicas, m \in messages :
         \/ /\ B!HandleBus(r, m) /\ UNCHANGED faultVars
         \/ /\ B!RejectFencedBus(r, m) /\ UNCHANGED faultVars
         \/ /\ B!HandleScheduleAnnouncement(r, m) /\ UNCHANGED faultVars
         \/ /\ B!HandleScheduleInstall(r, m) /\ UNCHANGED faultVars
         \/ /\ B!HandleSyncPrepare(r, m) /\ UNCHANGED faultVars
         \/ /\ B!HandleSyncReply(r, m) /\ UNCHANGED faultVars
         \/ /\ B!HandleSyncCommit(r, m) /\ UNCHANGED faultVars
    \/ \E l \in Replicas, m \in messages :
         /\ B!HandleScheduleAck(l, m) /\ UNCHANGED faultVars
    \/ \E l, r \in Replicas :
         /\ B!SendScheduleInstall(l, r) /\ UNCHANGED faultVars
    \/ \E l \in Replicas : /\ B!CommitSchedule(l) /\ UNCHANGED faultVars
    \/ \E r \in Replicas, s \in Slots :
         \/ /\ B!ExecuteSlot(r, s) /\ UNCHANGED faultVars
         \/ /\ B!MaybeCommitSync(r, s) /\ UNCHANGED faultVars
    \/ \E m \in messages : /\ B!ClientHandlesReply(m) /\ UNCHANGED faultVars
    \/ \E v \in RealValues : /\ B!ClientCommits(v) /\ UNCHANGED faultVars
    \/ \E a, b \in Replicas : /\ B!Reconnect(a, b) /\ UNCHANGED faultVars
    \/ \E r, l \in Replicas : /\ B!BuildViewChangeReport(r, l) /\ UNCHANGED faultVars
    \/ \E r, l \in Replicas : /\ B!InstallViewSchedule(r, l) /\ UNCHANGED faultVars
    \/ \E l \in Replicas, s \in Slots :
         \/ /\ B!FetchMergedSlot(l, s) /\ UNCHANGED faultVars
         \/ /\ B!CloseMergedSlotAsNoOp(l, s) /\ UNCHANGED faultVars
    \/ \E r \in Replicas, s \in Slots, v \in StoredValues :
         /\ B!InstallCanonicalSlot(r, s, v) /\ UNCHANGED faultVars
    \/ \E r \in Replicas : /\ B!FinishViewChange(r) /\ UNCHANGED faultVars
    \/ \E r \in Replicas :
         /\ ~donorAvailable[r] /\ B!SetDonorAvailable(r, TRUE) /\ UNCHANGED faultVars
    \/ \E r \in Replicas, v \in RealValues, s \in Slots :
         /\ B!HandleReboardedBus(r, v, s) /\ UNCHANGED faultVars
    \/ \E a, b \in Replicas, s \in Slots :
         /\ B!ServeState(a, b, s) /\ UNCHANGED faultVars

MCNext ==
    \/ \E r \in Replicas, c \in Clients : MCClientAnnouncesSchedule(c, r)
    \/ \E c \in Clients, v \in RealValues, r \in Replicas : MCClientSendsBus(c, v, r)
    \/ \E r, q \in Replicas, s \in Slots : MCSendSyncPrepare(r, q, s)
    \/ \E a, b \in Replicas : MCDisconnect(a, b)
    \/ \E m \in messages : MCLoseMessage(m)
    \/ \E r \in Replicas : MCBeginViewChange(r)
    \/ \E r \in Replicas : MCSetDonorUnavailable(r)
    \/ \E r \in Replicas, s \in Slots : MCPruneCommittedSlot(r, s)
    \/ \E v \in RealValues : MCClientReboards(v)
    \/ ReactiveNext

MCSpec == MCInit /\ [][MCNext]_mcVars

\* ReplicaOrder fixes identities, so only the identity permutation is sound.
IdentityPermutation == [r \in Replicas |-> r]
Symmetry == {IdentityPermutation}
MsgBufferConstraint == Cardinality(messages) <= MaxMsgBufferLimit

MCTypeOK == TypeOK /\ constraintCounters \in
    [registration : Nat, bus : Nat, sync : Nat, loss : Nat, connection : Nat,
     viewChange : Nat, donorFailure : Nat, prune : Nat, reboard : Nat]

StableMonotonic == \A r \in Replicas : stable'[r] >= stable[r]
ExecutionMonotonicOrRewind ==
    \A r \in Replicas : executedThrough'[r] >= executedThrough[r] \/ replicaStatus[r] = "view-change"
=============================================================================
