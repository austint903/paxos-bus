------------------------------ MODULE Trace ------------------------------
\* Category A linear trace replay for PaxosBus @ 07d9b4e.
EXTENDS base, Json, IOUtils, Sequences, TLC

JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON
    ELSE "traces/schedule-partial-zero.ndjson"

TraceLog == TLCEval(
    LET all == ndJsonDeserialize(JsonFile)
    IN SelectSeq(all, LAMBDA x :
         /\ "tag" \in DOMAIN x /\ x.tag = "trace"
         /\ "event" \in DOMAIN x))

ASSUME Len(TraceLog) > 0

VARIABLE l
traceVars == <<l>>
traceAllVars == <<vars, traceVars>>
logline == TraceLog[l].event

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
MembersOf(x) == CASE x = "both" -> Clients
                     [] x = "c0" -> {ClientOrder[1]}
                     [] x = "c1" -> {ClientOrder[2]}
                     [] OTHER -> {}
IntervalsOf(e) == [c \in Clients |->
    IF c = ClientOrder[1] THEN e.i0 ELSE e.i1]

TraceMessage(e) ==
    Msg(e.msg.kind, ActorOf(e.msg.src), ActorOf(e.msg.dst), e.msg.view,
        e.msg.slot, ValueOf(e.msg.value), e.msg.epoch, e.msg.window,
        MembersOf(e.msg.members), IntervalsOf(e.msg))

\* Strong post-state checks match instrumentation-spec.md's common snapshot.
\* Each event records the fields its action changes; sentinels guard unrelated maps.
ValidatePostState(e) ==
    LET r == ReplicaOf(e.r)
        q == ReplicaOf(e.q)
        c == ClientOf(e.client)
        s == e.slot
        w == e.window
        x == ValueOf(e.value)
        st == e.state
    IN
    /\ Cardinality(messages') = st.messageCount
    /\ view'[r] = st.view
    /\ replicaStatus'[r] = st.status
    /\ executedThrough'[r] = st.executedThrough
    /\ stableSlot'[r] = st.stableSlot
    /\ donorAvailable'[r] = st.donorAvailable
    /\ committedEnd'[r] = st.committedEnd
    /\ scheduleEpoch'[r] = st.scheduleEpoch
    /\ preparedWindow'[r] = st.preparedWindow
    /\ Cardinality(preparedAcks'[r]) = st.preparedAckCount
    /\ clientPauseView'[c] = st.clientPauseView
    /\ clientActiveView'[c] = st.clientActiveView
    /\ clientPaused'[c] = st.clientPaused
    /\ controlConnUp'[c][r] = st.controlConnUp
    /\ dataConnUp'[c][r] = st.dataConnUp
    /\ connectionUp'[r][q] = st.connectionUp
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
          ELSE TRUE
    /\ IF w \in Windows
          THEN /\ (w \in scheduleHistory'[r]) = st.inScheduleHistory
               /\ Cardinality(windowMembers'[r][w]) = st.windowMemberCount
               /\ windowIntervals'[r][w][ClientOrder[1]] = st.interval0
               /\ windowIntervals'[r][w][ClientOrder[2]] = st.interval1
          ELSE TRUE
    /\ IF x \in RealValues
          THEN /\ Cardinality(deliveredReplies'[x]) = st.deliveredReplyCount
               /\ (x \in clientCommitted') = st.clientCommitted
          ELSE TRUE

BaseStep(e) ==
    LET r == ReplicaOf(e.r)
        q == ReplicaOf(e.q)
        c == ClientOf(e.client)
        w == e.window
        s == e.slot
        x == ValueOf(e.value)
        m == TraceMessage(e)
    IN CASE e.name = "RequestScheduleWindow" -> RequestScheduleWindow(c,r,w,e.interval)
       [] e.name = "LeaderPrepareSchedule" -> LeaderPrepareSchedule(r,w)
       [] e.name = "HandleSchedulePrepare" -> HandleSchedulePrepare(r,m)
       [] e.name = "HandleScheduleReply" -> HandleScheduleReply(r,m)
       [] e.name = "LeaderCommitSchedule" -> LeaderCommitSchedule(r)
       [] e.name = "HandleScheduleCommit" -> HandleScheduleCommit(r,m)
       [] e.name = "ClientSendsBus" -> ClientSendsBus(c,x,r,w)
       [] e.name = "HandleBus" -> HandleBus(r,m)
       [] e.name = "ExecuteSlot" -> ExecuteSlot(r,s)
       [] e.name = "ClientHandlesReply" -> ClientHandlesReply(c,m)
       [] e.name = "ClientCommits" -> ClientCommits(c,x)
       [] e.name = "SendSyncPrepare" -> SendSyncPrepare(r,q,s)
       [] e.name = "HandleSyncPrepare" -> HandleSyncPrepare(r,m)
       [] e.name = "HandleSyncReply" -> HandleSyncReply(r,m)
       [] e.name = "MaybeCommitSync" -> MaybeCommitSync(r,s)
       [] e.name = "HandleSyncCommit" -> HandleSyncCommit(r,m)
       [] e.name = "Disconnect" -> Disconnect(r,q)
       [] e.name = "Reconnect" -> Reconnect(r,q)
       [] e.name = "LoseMessage" -> LoseMessage(m)
       [] e.name = "BeginViewChange" -> BeginViewChange(r)
       [] e.name = "BuildViewChangeReport" -> BuildViewChangeReport(r,q)
       [] e.name = "FetchMergedSlot" -> FetchMergedSlot(r,s)
       [] e.name = "CloseMergedSlotAsNoOp" -> CloseMergedSlotAsNoOp(r,s)
       [] e.name = "InstallCanonicalSlot" -> InstallCanonicalSlot(r,s,x)
       [] e.name = "FinishViewChange" -> FinishViewChange(r)
       [] e.name = "SetDonorAvailable" -> SetDonorAvailable(r,e.flag)
       [] e.name = "PruneCommittedSlot" -> PruneCommittedSlot(r,s)
       [] e.name = "ServeState" -> ServeState(r,q,s)
       [] e.name = "SetControlConnection" -> SetControlConnection(c,r,e.flag)
       [] e.name = "HandleControlReport" -> HandleControlReport(c,r)
       [] e.name = "PauseForReportedView" -> PauseForReportedView(c,r)
       [] e.name = "ResumeCertifiedView" -> ResumeCertifiedView(c,e.view)
       [] e.name = "FailDataConnection" -> FailDataConnection(c,r)
       [] OTHER -> FALSE

TraceInit == /\ Init /\ l = 1

MatchEvent ==
    /\ l <= Len(TraceLog)
    /\ BaseStep(logline)
    /\ ValidatePostState(logline)
    /\ l' = l + 1

\* No silent protocol transitions are admitted: every modeled async boundary
\* has a dedicated event. Only terminal stuttering is allowed.
TraceNext ==
    \/ MatchEvent
    \/ /\ l > Len(TraceLog) /\ UNCHANGED traceAllVars

TraceSpec == TraceInit /\ [][TraceNext]_traceAllVars /\ WF_traceAllVars(MatchEvent)
TraceMatched == <>(l > Len(TraceLog))
=============================================================================
