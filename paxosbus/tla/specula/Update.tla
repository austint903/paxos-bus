----------------------------- MODULE Update -----------------------------
\* Incremental checking lens for the leader-coordinated schedule protocol in
\* PaxosBus revision bf46b074c98f475351e3fe257dbcfac5123daf19.
\* The complete implementation behavior remains owned by base/MC.

EXTENDS MC

\* Scenario U1: leader collection -> quorum installation -> client release.
\* reference: base!ClientAnnouncesSchedule, base!HandleScheduleAnnouncement,
\*            base!SendScheduleInstall, base!HandleScheduleInstall,
\*            base!HandleScheduleAck, base!CommitSchedule
\* code: paxosbus/client.go:awaitInitialSchedule;
\*       paxosbus/schedule.go:handleSync,replicateSchedule,
\*       handleScheduleInstall,handleScheduleAck,coordinateInitialSchedule
AffectedActions ==
    \/ \E c \in Clients, r \in Replicas : MCClientAnnouncesSchedule(c, r)
    \/ \E r \in Replicas, m \in messages :
         \/ /\ B!HandleScheduleAnnouncement(r, m) /\ UNCHANGED faultVars
         \/ /\ B!HandleScheduleInstall(r, m) /\ UNCHANGED faultVars
    \/ \E l, r \in Replicas :
         /\ B!SendScheduleInstall(l, r) /\ UNCHANGED faultVars
    \/ \E l \in Replicas, m \in messages :
         /\ B!HandleScheduleAck(l, m) /\ UNCHANGED faultVars
    \/ \E l \in Replicas : /\ B!CommitSchedule(l) /\ UNCHANGED faultVars
    \/ \E r, l \in Replicas :
         /\ B!InstallViewSchedule(r, l) /\ UNCHANGED faultVars

\* Scenarios U2-U4: leader loss during collection, carried schedules during
\* view change, lagging replica fencing, and post-install bus admission.
\* reference: base!BeginViewChange, base!BuildViewChangeReport,
\*            base!FinishViewChange, base!ClientSendsBus, base!HandleBus,
\*            base!ExecuteSlot,
\*            base!RejectFencedBus, base!LoseMessage, base!Disconnect
\* code: paxosbus/recovery.go:beginViewChangeLocked,buildViewChangeLocked,
\*       mergeSuffix,driveViewChange,installStartView,publishNormalViewLocked;
\*       paxosbus/replica.go:admitLocked,handleBus
InteractionActions ==
    \/ \E c \in Clients, v \in RealValues, r \in Replicas : MCClientSendsBus(c, v, r)
    \/ \E r \in Replicas, m \in messages :
         \/ /\ B!HandleBus(r, m) /\ UNCHANGED faultVars
         \/ /\ B!RejectFencedBus(r, m) /\ UNCHANGED faultVars
    \/ \E r \in Replicas, s \in Slots :
         /\ B!ExecuteSlot(r, s) /\ UNCHANGED faultVars
    \/ \E r \in Replicas : MCBeginViewChange(r)
    \/ \E r, l \in Replicas :
         \/ /\ B!BuildViewChangeReport(r, l) /\ UNCHANGED faultVars
    \/ \E r \in Replicas : /\ B!FinishViewChange(r) /\ UNCHANGED faultVars
    \/ \E m \in messages : MCLoseMessage(m)
    \/ \E a, b \in Replicas : MCDisconnect(a, b)
    \/ \E a, b \in Replicas : /\ B!Reconnect(a, b) /\ UNCHANGED faultVars

ConcreteUpdateNext == AffectedActions \/ InteractionActions
ConcreteUpdateSpec == MCInit /\ [][ConcreteUpdateNext]_mcVars

\* Full-reference checking remains authoritative; focused checking only
\* accelerates exploration of the update boundary.
FullUpdateSpec == MCSpec

UpdateScheduleSafety ==
    /\ ScheduleOwnershipStable
    /\ ReadyReplicaHasCompleteSchedule
    /\ ReleasedOnlyAfterScheduleCommit
    /\ NormalViewPreservesCommittedSchedule

\* The new source admits only an empty schedule or the exact complete client
\* schedule; a partial per-replica membership set was the original defect.
NoPartialInstalledSchedule ==
    \A r \in Replicas : registered[r] \in {{}, Clients}

=============================================================================
