------------------------------- MODULE MC -------------------------------
\* Counter-bounded environment for the five brief scenarios.
EXTENDS base
B == INSTANCE base

CONSTANTS ScheduleRequestLimit, BusLimit, SyncLimit, LossLimit,
          DisconnectLimit, ViewChangeLimit, DonorFailureLimit,
          PruneLimit, ControlReportLimit, DataFailureLimit,
          MaxMsgBufferLimit

ASSUME /\ ScheduleRequestLimit \in Nat /\ BusLimit \in Nat
       /\ SyncLimit \in Nat /\ LossLimit \in Nat
       /\ DisconnectLimit \in Nat /\ ViewChangeLimit \in Nat
       /\ DonorFailureLimit \in Nat /\ PruneLimit \in Nat
       /\ ControlReportLimit \in Nat /\ DataFailureLimit \in Nat
       /\ MaxMsgBufferLimit \in Nat

VARIABLE counters
faultVars == <<counters>>
mcVars == <<vars, faultVars>>

MCRequestScheduleWindow(c,l,w,i) ==
    /\ counters.scheduleRequest < ScheduleRequestLimit
    /\ B!RequestScheduleWindow(c,l,w,i)
    /\ counters' = [counters EXCEPT !.scheduleRequest = @ + 1]

MCClientSendsBus(c,x,r,w) ==
    /\ counters.bus < BusLimit
    /\ B!ClientSendsBus(c,x,r,w)
    /\ counters' = [counters EXCEPT !.bus = @ + 1]

MCSendSyncPrepare(l,r,s) ==
    /\ counters.sync < SyncLimit
    /\ B!SendSyncPrepare(l,r,s)
    /\ counters' = [counters EXCEPT !.sync = @ + 1]

MCLoseMessage(m) ==
    /\ counters.loss < LossLimit /\ B!LoseMessage(m)
    /\ counters' = [counters EXCEPT !.loss = @ + 1]

MCDisconnect(a,b) ==
    /\ counters.disconnect < DisconnectLimit /\ B!Disconnect(a,b)
    /\ counters' = [counters EXCEPT !.disconnect = @ + 1]

MCBeginViewChange(r) ==
    /\ counters.viewChange < ViewChangeLimit /\ B!BeginViewChange(r)
    /\ counters' = [counters EXCEPT !.viewChange = @ + 1]

MCSetDonorUnavailable(r) ==
    /\ counters.donorFailure < DonorFailureLimit /\ donorAvailable[r]
    /\ B!SetDonorAvailable(r,FALSE)
    /\ counters' = [counters EXCEPT !.donorFailure = @ + 1]

MCPruneCommittedSlot(r,s) ==
    /\ counters.prune < PruneLimit /\ B!PruneCommittedSlot(r,s)
    /\ counters' = [counters EXCEPT !.prune = @ + 1]

MCHandleControlReport(c,r) ==
    /\ counters.controlReport < ControlReportLimit
    /\ B!HandleControlReport(c,r)
    /\ counters' = [counters EXCEPT !.controlReport = @ + 1]

MCFailDataConnection(c,r) ==
    /\ counters.dataFailure < DataFailureLimit
    /\ B!FailDataConnection(c,r)
    /\ counters' = [counters EXCEPT !.dataFailure = @ + 1]

MCInit ==
    /\ Init
    /\ counters = [scheduleRequest |-> 0, bus |-> 0, sync |-> 0,
                    loss |-> 0, disconnect |-> 0, viewChange |-> 0,
                    donorFailure |-> 0, prune |-> 0,
                    controlReport |-> 0, dataFailure |-> 0]

ReactiveNext ==
    \/ \E l \in Replicas, w \in Windows : /\ B!LeaderPrepareSchedule(l,w) /\ UNCHANGED faultVars
    \/ \E r \in Replicas, m \in messages :
         \/ /\ B!HandleSchedulePrepare(r,m) /\ UNCHANGED faultVars
         \/ /\ B!HandleScheduleCommit(r,m) /\ UNCHANGED faultVars
         \/ /\ B!HandleBus(r,m) /\ UNCHANGED faultVars
         \/ /\ B!HandleSyncPrepare(r,m) /\ UNCHANGED faultVars
         \/ /\ B!HandleSyncCommit(r,m) /\ UNCHANGED faultVars
    \/ \E l \in Replicas, m \in messages :
         \/ /\ B!HandleScheduleReply(l,m) /\ UNCHANGED faultVars
         \/ /\ B!HandleSyncReply(l,m) /\ UNCHANGED faultVars
    \/ \E l \in Replicas : /\ B!LeaderCommitSchedule(l) /\ UNCHANGED faultVars
    \/ \E r \in Replicas, s \in Slots : /\ B!ExecuteSlot(r,s) /\ UNCHANGED faultVars
    \/ \E c \in Clients, m \in messages : /\ B!ClientHandlesReply(c,m) /\ UNCHANGED faultVars
    \/ \E c \in Clients, x \in RealValues : /\ B!ClientCommits(c,x) /\ UNCHANGED faultVars
    \/ \E l \in Replicas, s \in Slots : /\ B!MaybeCommitSync(l,s) /\ UNCHANGED faultVars
    \/ \E a,b \in Replicas : /\ ~connectionUp[a][b] /\ B!Reconnect(a,b) /\ UNCHANGED faultVars
    \/ \E r,l \in Replicas : /\ B!BuildViewChangeReport(r,l) /\ UNCHANGED faultVars
    \/ \E l \in Replicas, s \in Slots :
         \/ /\ B!FetchMergedSlot(l,s) /\ UNCHANGED faultVars
         \/ /\ B!CloseMergedSlotAsNoOp(l,s) /\ UNCHANGED faultVars
    \/ \E r \in Replicas, s \in Slots, x \in StoredValues :
         /\ B!InstallCanonicalSlot(r,s,x) /\ UNCHANGED faultVars
    \/ \E r \in Replicas : /\ B!FinishViewChange(r) /\ UNCHANGED faultVars
    \/ \E r \in Replicas : /\ ~donorAvailable[r] /\ B!SetDonorAvailable(r,TRUE) /\ UNCHANGED faultVars
    \/ \E a,b \in Replicas, s \in Slots : /\ B!ServeState(a,b,s) /\ UNCHANGED faultVars
    \/ \E c \in Clients, r \in Replicas, up \in BOOLEAN :
         /\ B!SetControlConnection(c,r,up) /\ UNCHANGED faultVars
    \/ \E c \in Clients, r \in Replicas : /\ B!PauseForReportedView(c,r) /\ UNCHANGED faultVars
    \/ \E c \in Clients, v \in Views : /\ B!ResumeCertifiedView(c,v) /\ UNCHANGED faultVars

MCNext ==
    \/ \E c \in Clients, l \in Replicas, w \in Windows, i \in 0..1 : MCRequestScheduleWindow(c,l,w,i)
    \/ \E c \in Clients, x \in RealValues, r \in Replicas, w \in Windows : MCClientSendsBus(c,x,r,w)
    \/ \E l,r \in Replicas, s \in Slots : MCSendSyncPrepare(l,r,s)
    \/ \E m \in messages : MCLoseMessage(m)
    \/ \E a,b \in Replicas : MCDisconnect(a,b)
    \/ \E r \in Replicas : MCBeginViewChange(r)
    \/ \E r \in Replicas : MCSetDonorUnavailable(r)
    \/ \E r \in Replicas, s \in Slots : MCPruneCommittedSlot(r,s)
    \/ \E c \in Clients, r \in Replicas : MCHandleControlReport(c,r)
    \/ \E c \in Clients, r \in Replicas : MCFailDataConnection(c,r)
    \/ ReactiveNext

MCSpec == MCInit /\ [][MCNext]_mcVars

IdentityPermutation == [r \in Replicas |-> r]
Symmetry == {IdentityPermutation}
MsgBufferConstraint == Cardinality(messages) <= MaxMsgBufferLimit
MCTypeOK == TypeOK /\ counters \in
    [scheduleRequest : Nat, bus : Nat, sync : Nat, loss : Nat,
     disconnect : Nat, viewChange : Nat, donorFailure : Nat, prune : Nat,
     controlReport : Nat, dataFailure : Nat]

\* Structural invariants used for convergence.
CommittedFrontierHasHistory ==
    \A r \in Replicas : \A w \in 1..committedEnd[r] : w \in scheduleHistory[r]
PreparedWindowInHistory ==
    \A r \in Replicas : preparedWindow[r] # 0 => preparedWindow[r] \in scheduleHistory[r]

\* Liveness properties from brief §5; enable only with an appropriate fair hunt.
ClientRequestProgress ==
    \A x \in RealValues : (x \in clientCommitted) \/ <>(x \in clientCommitted)
ScheduleWindowProgress ==
    \A r \in Replicas, w \in Windows :
      w \in scheduleHistory[r] => <>(w <= committedEnd[r] \/ view[r] > scheduleEpoch[r])
=============================================================================
