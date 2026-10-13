------------------------------ MODULE Trace ------------------------------
\* Category A linear trace replay for PaxosBus.

EXTENDS base, Json, IOUtils, Sequences, TLC

JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON
    ELSE "../traces/trace.ndjson"

TraceLog == TLCEval(
    LET all == ndJsonDeserialize(JsonFile)
    IN SelectSeq(all, LAMBDA x :
        /\ "tag" \in DOMAIN x /\ x.tag = "trace"
        /\ "event" \in DOMAIN x))

ASSUME Len(TraceLog) > 0

VARIABLE l
traceVars == <<l>>
traceAllVars == <<vars, traceVars>>
logline == TraceLog[l]

ReplicaOf(x) == CASE x = "r0" -> ReplicaOrder[1]
                     [] x = "r1" -> ReplicaOrder[2]
                     [] OTHER -> ReplicaOrder[3]
ClientOf(x) == IF x = "c0" THEN ClientOrder[1] ELSE ClientOrder[2]
ActorOf(x) == CASE x = "r0" -> ReplicaOrder[1]
                   [] x = "r1" -> ReplicaOrder[2]
                   [] x = "r2" -> ReplicaOrder[3]
                   [] x = "c0" -> ClientOrder[1]
                   [] OTHER -> ClientOrder[2]
ValueOf(x) == CASE x = "BusA" -> BusA
                   [] x = "BusB" -> BusB
                   [] x = "NoOp" -> NoOp
                   [] OTHER -> Empty
DonorOf(x) == IF x = "none" THEN NoReplica ELSE ReplicaOf(x)

TraceMessage(e) ==
    Msg(e.msg.kind, ActorOf(e.msg.src), ActorOf(e.msg.dst), e.msg.view,
        e.msg.slot, ValueOf(e.msg.value), e.msg.epoch)

\* Strong post-state validation. The instrumentation schema captures all fields
\* touched by any action; slot/value-specific checks are guarded for sentinel args.
ValidatePostState(e) ==
    LET r == ReplicaOf(e.r)
        q == ReplicaOf(e.q)
        s == e.slot
        v == ValueOf(e.value)
        st == e.state
    IN
    /\ Cardinality(messages') = st.messageCount
    /\ executedThrough'[r] = st.executedThrough
    /\ stable'[r] = st.stable
    /\ view'[r] = st.view
    /\ replicaStatus'[r] = st.status
    /\ donorAvailable'[r] = st.donorAvailable
    /\ nextIndex'[r] = st.nextIndex
    /\ Cardinality(registered'[r]) = st.registeredCount
    /\ Cardinality(scheduleAnnouncements'[r]) = st.scheduleAnnouncementCount
    /\ scheduleEpoch'[r] = st.scheduleEpoch
    /\ scheduleReady'[r] = st.scheduleReady
    /\ scheduleInstallActive'[r] = st.scheduleInstallActive
    /\ Cardinality(scheduleAcks'[r]) = st.scheduleAckCount
    /\ scheduleCommitted' = st.scheduleCommitted
    /\ Cardinality(releasedClients') = st.releasedClientCount
    /\ connectionUp'[r][q] = st.connectionUp
    /\ connectionEpoch'[r][q] = st.connectionEpoch
    /\ Cardinality(reportReceived'[q]) = st.reportCount
    /\ IF e.name = "BuildViewChangeReport"
          THEN Cardinality(reportSchedule'[q][r]) = st.reportScheduleCount
          ELSE TRUE
    /\ IF s \in Slots
          THEN /\ memLog'[r][s] = ValueOf(st.memValue)
               /\ canonicalLog'[s] = ValueOf(st.canonicalValue)
               /\ stableValue'[r][s] = ValueOf(st.stableValue)
               /\ prepareDisposition'[r][s] = st.prepareDisposition
               /\ prepareValue'[r][s] = ValueOf(st.prepareValue)
               /\ Cardinality(syncAcks'[r][s]) = st.syncAckCount
               /\ chosenDonor'[q][s] = DonorOf(st.chosenDonor)
               /\ fetchAttempts'[r][s] = st.fetchAttempts
               /\ diskLog'[r][s] = ValueOf(st.diskValue)
               /\ diskGeneration'[r][s] = st.diskGeneration
               /\ canonicalGeneration'[s] = st.canonicalGeneration
               /\ (s \in pruned'[r]) = st.pruned
               /\ transferValue'[r][s] = ValueOf(st.transferValue)
               /\ Cardinality(ownershipSeen'[s]) = st.ownershipCount
          ELSE TRUE
    /\ IF v \in RealValues
          THEN /\ Cardinality(generatedReplies'[v]) = st.generatedReplyCount
               /\ Cardinality(deliveredReplies'[v]) = st.deliveredReplyCount
               /\ (v \in committedValues') = st.committed
               /\ dedupIndex'[r][v] = st.dedupIndex
               /\ Cardinality(requestIndexes'[v]) = st.requestIndexCount
               /\ executionCount'[r][v] = st.executionCount
               /\ (v \in pendingReboard') = st.pendingReboard
          ELSE TRUE

BaseStep(e) ==
    LET r == ReplicaOf(e.r)
        q == ReplicaOf(e.q)
        c == ClientOf(e.client)
        s == e.slot
        v == ValueOf(e.value)
        m == TraceMessage(e)
    IN CASE e.name = "ClientAnnouncesSchedule" -> ClientAnnouncesSchedule(c, r)
       [] e.name = "HandleScheduleAnnouncement" -> HandleScheduleAnnouncement(r, m)
       [] e.name = "SendScheduleInstall" -> SendScheduleInstall(r, q)
       [] e.name = "HandleScheduleInstall" -> HandleScheduleInstall(r, m)
       [] e.name = "HandleScheduleAck" -> HandleScheduleAck(r, m)
       [] e.name = "CommitSchedule" -> CommitSchedule(r)
       [] e.name = "ClientSendsBus" -> ClientSendsBus(c, v, r)
       [] e.name = "HandleBus" -> HandleBus(r, m)
       [] e.name = "RejectFencedBus" -> RejectFencedBus(r, m)
       [] e.name = "ExecuteSlot" -> ExecuteSlot(r, s)
       [] e.name = "ClientHandlesReply" -> ClientHandlesReply(m)
       [] e.name = "ClientCommits" -> ClientCommits(v)
       [] e.name = "SendSyncPrepare" -> SendSyncPrepare(r, q, s)
       [] e.name = "HandleSyncPrepare" -> HandleSyncPrepare(r, m)
       [] e.name = "HandleSyncReply" -> HandleSyncReply(r, m)
       [] e.name = "MaybeCommitSync" -> MaybeCommitSync(r, s)
       [] e.name = "HandleSyncCommit" -> HandleSyncCommit(r, m)
       [] e.name = "Disconnect" -> Disconnect(r, q)
       [] e.name = "Reconnect" -> Reconnect(r, q)
       [] e.name = "LoseMessage" -> LoseMessage(m)
       [] e.name = "BeginViewChange" -> BeginViewChange(r)
       [] e.name = "BuildViewChangeReport" -> BuildViewChangeReport(r, q)
       [] e.name = "InstallViewSchedule" -> InstallViewSchedule(r, q)
       [] e.name = "FetchMergedSlot" -> FetchMergedSlot(r, s)
       [] e.name = "CloseMergedSlotAsNoOp" -> CloseMergedSlotAsNoOp(r, s)
       [] e.name = "InstallCanonicalSlot" -> InstallCanonicalSlot(r, s, v)
       [] e.name = "FinishViewChange" -> FinishViewChange(r)
       [] e.name = "SetDonorAvailable" -> SetDonorAvailable(r, e.flag)
       [] e.name = "PruneCommittedSlot" -> PruneCommittedSlot(r, s)
       [] e.name = "ClientReboards" -> ClientReboards(v)
       [] e.name = "HandleReboardedBus" -> HandleReboardedBus(r, v, s)
       [] e.name = "ServeState" -> ServeState(r, q, s)
       [] OTHER -> FALSE

TraceInit == /\ Init /\ l = 1

MatchEvent ==
    /\ l <= Len(TraceLog)
    /\ BaseStep(logline.event)
    /\ ValidatePostState(logline.event)
    /\ l' = l + 1

TraceNext ==
    \/ MatchEvent
    \/ /\ l > Len(TraceLog)
       /\ UNCHANGED traceAllVars

\* Weak fairness prevents the stuttering admitted by [TraceNext]_traceAllVars
\* from postponing an enabled trace event forever.
TraceSpec == TraceInit /\ [][TraceNext]_traceAllVars /\ WF_traceAllVars(MatchEvent)
TraceMatched == <>(l > Len(TraceLog))
=============================================================================
