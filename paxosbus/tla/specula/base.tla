------------------------------ MODULE base ------------------------------
\* PaxosBus implementation model. Category A (distributed/message passing).
\* Every extension below is motivated by Modeling Brief Scenarios S1-S5.

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Replicas, ReplicaOrder, Clients, ClientOrder, r0, r1, r2, c0, c1,
          BusA, BusB, NoOp, Empty, NoReplica,
          MaxSlot, MaxView, RetainWindow, MetadataWindow, MaxFetchAttempts,
          DurableReplicas

ASSUME /\ IsFiniteSet(Replicas) /\ Cardinality(Replicas) % 2 = 1
       /\ ReplicaOrder \in Seq(Replicas) /\ Len(ReplicaOrder) = Cardinality(Replicas)
       /\ IsFiniteSet(Clients) /\ Cardinality(Clients) = 2
       /\ ClientOrder \in Seq(Clients) /\ Len(ClientOrder) = Cardinality(Clients)
       /\ MaxSlot \in Nat \ {0} /\ MaxView \in Nat
       /\ RetainWindow \in Nat /\ MetadataWindow \in Nat \ {0}
       /\ MaxFetchAttempts \in Nat \ {0}
       /\ DurableReplicas \subseteq Replicas

Slots == 1..MaxSlot
Views == 0..MaxView
RealValues == {BusA, BusB}
LogValues == RealValues \cup {NoOp, Empty}
StoredValues == LogValues \ {Empty}
Evidence == {"present", "absent", "unknown"}
Disposition == {"none", "match", "mismatch", "unverified"}
Status == {"normal", "view-change"}
MessageKinds == {"bus", "reply", "sync-prepare", "sync-reply", "sync-commit",
                  "view-report", "start-view", "schedule-announce",
                  "schedule-install", "schedule-ack"}

Quorum(S) == Cardinality(S) * 2 > Cardinality(Replicas)
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Leader(v) == ReplicaOrder[(v % Len(ReplicaOrder)) + 1]
Rank(c) == CHOOSE i \in 1..Len(ClientOrder) : ClientOrder[i] = c
BusOwner(v) == IF v = BusA THEN ClientOrder[1] ELSE ClientOrder[2]

\* replica.go:109-122: membership-sensitive ordering calculation (Scenario 5).
ScheduledSlot(reg, r, v) ==
    LET c == BusOwner(v)
        before == Cardinality({d \in reg[r] : Rank(d) < Rank(c)})
    IN 1 + before

Message == [kind : MessageKinds, src : Replicas \cup Clients,
            dst : Replicas \cup Clients, view : Views, slot : 0..MaxSlot,
            value : LogValues, epoch : Nat]

VARIABLES
    messages,
    connectionUp, connectionEpoch,
    memLog, canonicalLog, valueHistory, executedThrough,
    stable, stableValue,
    view, replicaStatus,
    prepareDisposition, prepareValue, syncAcks,
    reportReceived, reportEvidence, reportSchedule, chosenDonor, donorAvailable, fetchAttempts,
    diskLog, diskGeneration, canonicalGeneration, pruned, transferValue,
    transferFromDisk,
    generatedReplies, deliveredReplies, committedValues,
    dedupIndex, requestIndexes, executionCount, nextIndex, pendingReboard,
    registered, ownershipSeen, scheduleAnnouncements, scheduleEpoch,
    scheduleReady, scheduleInstallActive, scheduleAcks, scheduleCommitted,
    releasedClients

networkVars == <<messages, connectionUp, connectionEpoch>>
logVars == <<memLog, canonicalLog, valueHistory, executedThrough, stable, stableValue>>
syncVars == <<prepareDisposition, prepareValue, syncAcks>>
viewVars == <<view, replicaStatus, reportReceived, reportEvidence, reportSchedule,
              chosenDonor, donorAvailable, fetchAttempts>>
diskVars == <<diskLog, diskGeneration, canonicalGeneration, pruned, transferValue,
              transferFromDisk>>
clientVars == <<generatedReplies, deliveredReplies, committedValues, dedupIndex,
                requestIndexes, executionCount, nextIndex, pendingReboard>>
scheduleVars == <<registered, ownershipSeen, scheduleAnnouncements, scheduleEpoch,
                  scheduleReady, scheduleInstallActive, scheduleAcks,
                  scheduleCommitted, releasedClients>>
vars == <<networkVars, logVars, syncVars, viewVars, diskVars, clientVars, scheduleVars>>

Init ==
    /\ messages = {}
    /\ connectionUp = [r \in Replicas |-> [q \in Replicas |-> r # q]]
    /\ connectionEpoch = [r \in Replicas |-> [q \in Replicas |-> 0]]
    /\ memLog = [r \in Replicas |-> [s \in Slots |-> Empty]]
    /\ canonicalLog = [s \in Slots |-> Empty]
    /\ valueHistory = [s \in Slots |-> {}]
    /\ executedThrough = [r \in Replicas |-> 0]
    /\ stable = [r \in Replicas |-> 0]
    /\ stableValue = [r \in Replicas |-> [s \in Slots |-> Empty]]
    /\ view = [r \in Replicas |-> 0]
    /\ replicaStatus = [r \in Replicas |-> "normal"]
    /\ prepareDisposition = [r \in Replicas |-> [s \in Slots |-> "none"]]
    /\ prepareValue = [r \in Replicas |-> [s \in Slots |-> Empty]]
    /\ syncAcks = [r \in Replicas |-> [s \in Slots |-> {}]]
    /\ reportReceived = [r \in Replicas |-> {}]
    /\ reportEvidence = [r \in Replicas |-> [q \in Replicas |-> [s \in Slots |-> "unknown"]]]
    /\ reportSchedule = [r \in Replicas |-> [q \in Replicas |-> {}]]
    /\ chosenDonor = [r \in Replicas |-> [s \in Slots |-> NoReplica]]
    /\ donorAvailable = [r \in Replicas |-> TRUE]
    /\ fetchAttempts = [r \in Replicas |-> [s \in Slots |-> 0]]
    /\ diskLog = [r \in Replicas |-> [s \in Slots |-> Empty]]
    /\ diskGeneration = [r \in Replicas |-> [s \in Slots |-> 0]]
    /\ canonicalGeneration = [s \in Slots |-> 0]
    /\ pruned = [r \in Replicas |-> {}]
    /\ transferValue = [r \in Replicas |-> [s \in Slots |-> Empty]]
    /\ transferFromDisk = [r \in Replicas |-> [s \in Slots |-> FALSE]]
    /\ generatedReplies = [v \in RealValues |-> {}]
    /\ deliveredReplies = [v \in RealValues |-> {}]
    /\ committedValues = {}
    /\ dedupIndex = [r \in Replicas |-> [v \in RealValues |-> 0]]
    /\ requestIndexes = [v \in RealValues |-> {}]
    /\ executionCount = [r \in Replicas |-> [v \in RealValues |-> 0]]
    /\ nextIndex = [r \in Replicas |-> 1]
    /\ pendingReboard = {}
    /\ registered = [r \in Replicas |-> {}]
    /\ ownershipSeen = [s \in Slots |-> {}]
    /\ scheduleAnnouncements = [r \in Replicas |-> {}]
    /\ scheduleEpoch = [r \in Replicas |-> 0]
    /\ scheduleReady = [r \in Replicas |-> FALSE]
    /\ scheduleInstallActive = [r \in Replicas |-> FALSE]
    /\ scheduleAcks = [r \in Replicas |-> {}]
    /\ scheduleCommitted = FALSE
    /\ releasedClients = {}

Send(m) == messages' = messages \cup {m}
Consume(m) == messages' = messages \ {m}
Msg(k, a, b, v, s, x, e) ==
    [kind |-> k, src |-> a, dst |-> b, view |-> v, slot |-> s, value |-> x, epoch |-> e]

\* UPDATE-MAP
\* code: paxosbus/client.go:awaitInitialSchedule
\* update: Update!AffectedActions
\* A client retries one immutable schedule announcement against the current
\* normal-view leader. Timing fields are abstracted into the client identity:
\* the implementation never changes them while retrying across views.
ClientAnnouncesSchedule(c, r) ==
    /\ r = Leader(view[r]) /\ replicaStatus[r] = "normal"
    /\ Send(Msg("schedule-announce", c, r, view[r], 0, Empty, 0))
    /\ UNCHANGED <<connectionUp, connectionEpoch, logVars, syncVars, viewVars,
                    diskVars, clientVars, scheduleVars>>

\* UPDATE-MAP
\* code: paxosbus/schedule.go:handleSync
\* update: Update!AffectedActions
\* The last distinct announcement atomically installs the complete schedule on
\* the leader in a fenced state and starts quorum replication.
HandleScheduleAnnouncement(r, m) ==
    /\ m \in messages /\ m.kind = "schedule-announce" /\ m.dst = r
    /\ m.view = view[r] /\ r = Leader(view[r]) /\ replicaStatus[r] = "normal"
    /\ ~scheduleReady[r] /\ ~scheduleInstallActive[r]
    /\ m.src \in Clients
    /\ LET next == scheduleAnnouncements[r] \cup {m.src} IN
       /\ scheduleAnnouncements' = [scheduleAnnouncements EXCEPT ![r] = next]
       /\ IF next = Clients
             THEN /\ registered' = [registered EXCEPT ![r] = Clients]
                  /\ scheduleEpoch' = [scheduleEpoch EXCEPT ![r] = view[r]]
                  /\ scheduleInstallActive' = [scheduleInstallActive EXCEPT ![r] = TRUE]
                  /\ scheduleAcks' = [scheduleAcks EXCEPT ![r] = {r}]
             ELSE UNCHANGED <<registered, scheduleEpoch, scheduleInstallActive,
                              scheduleAcks>>
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, connectionEpoch, logVars, syncVars, viewVars,
                    diskVars, clientVars, ownershipSeen, scheduleReady,
                    scheduleCommitted, releasedClients>>

\* UPDATE-MAP
\* code: paxosbus/schedule.go:replicateSchedule
\* update: Update!AffectedActions
SendScheduleInstall(l, r) ==
    /\ l # r /\ l = Leader(view[l]) /\ replicaStatus[l] = "normal"
    /\ registered[l] = Clients /\ scheduleEpoch[l] = view[l]
    /\ scheduleInstallActive[l] \/ scheduleReady[l]
    /\ connectionUp[l][r]
    /\ Send(Msg("schedule-install", l, r, view[l], 0, Empty, connectionEpoch[l][r]))
    /\ UNCHANGED <<connectionUp, connectionEpoch, logVars, syncVars, viewVars,
                    diskVars, clientVars, scheduleVars>>

\* UPDATE-MAP
\* code: paxosbus/schedule.go:handleScheduleInstall
\* update: Update!AffectedActions
\* Followers install only a current-view leader schedule and become eligible to
\* admit buses immediately; their acknowledgement witnesses quorum installation.
HandleScheduleInstall(r, m) ==
    /\ m \in messages /\ m.kind = "schedule-install" /\ m.dst = r
    /\ m.view = view[r] /\ m.src = Leader(view[r])
    /\ replicaStatus[r] = "normal"
    /\ registered[r] \in {{}, Clients}
    /\ registered' = [registered EXCEPT ![r] = Clients]
    /\ scheduleEpoch' = [scheduleEpoch EXCEPT ![r] = view[r]]
    /\ scheduleReady' = [scheduleReady EXCEPT ![r] = TRUE]
    /\ messages' = (messages \ {m}) \cup
         {Msg("schedule-ack", r, m.src, view[r], 0, Empty, m.epoch)}
    /\ UNCHANGED <<connectionUp, connectionEpoch, logVars, syncVars, viewVars,
                    diskVars, clientVars, ownershipSeen, scheduleAnnouncements,
                    scheduleInstallActive, scheduleAcks, scheduleCommitted,
                    releasedClients>>

\* UPDATE-MAP
\* code: paxosbus/schedule.go:handleScheduleAck
\* update: Update!AffectedActions
HandleScheduleAck(l, m) ==
    /\ m \in messages /\ m.kind = "schedule-ack" /\ m.dst = l
    /\ l = Leader(view[l]) /\ replicaStatus[l] = "normal"
    /\ (scheduleInstallActive[l] \/ scheduleReady[l]) /\ m.view = view[l]
    /\ scheduleAcks' = [scheduleAcks EXCEPT ![l] = @ \cup {m.src}]
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, connectionEpoch, logVars, syncVars, viewVars,
                    diskVars, clientVars, registered, ownershipSeen,
                    scheduleAnnouncements, scheduleEpoch, scheduleReady,
                    scheduleInstallActive, scheduleCommitted, releasedClients>>

\* UPDATE-MAP
\* code: paxosbus/schedule.go:coordinateInitialSchedule
\* update: Update!AffectedActions
\* The leader releases clients after an intersecting replica quorum, without
\* waiting for every laggard; laggards remain fenced by HandleBus.
CommitSchedule(l) ==
    /\ l = Leader(view[l]) /\ replicaStatus[l] = "normal"
    /\ scheduleInstallActive[l] /\ Quorum(scheduleAcks[l])
    /\ scheduleReady' = [scheduleReady EXCEPT ![l] = TRUE]
    /\ scheduleInstallActive' = [scheduleInstallActive EXCEPT ![l] = FALSE]
    /\ scheduleCommitted' = TRUE
    /\ releasedClients' = Clients
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskVars, clientVars,
                    registered, ownershipSeen, scheduleAnnouncements,
                    scheduleEpoch, scheduleAcks>>

ScheduleActive(r) ==
    /\ scheduleReady[r]
    /\ scheduleEpoch[r] = view[r]
    /\ registered[r] = Clients

\* client.go:sendBus and replica.go:handleBus: multicast one client bus (S4/update S5).
ClientSendsBus(c, v, r) ==
    /\ BusOwner(v) = c
    /\ c \in releasedClients
    /\ connectionUp[Leader(0)][r] \/ r = Leader(0)
    /\ Send(Msg("bus", c, r, view[r], 0, v, connectionEpoch[r][r]))
    /\ UNCHANGED <<logVars, syncVars, viewVars, diskVars, clientVars, scheduleVars,
                    connectionUp, connectionEpoch>>

\* replica.go:851-867,935-951,993-999: calculate then claim only an empty slot.
HandleBus(r, m) ==
    LET s == ScheduledSlot(registered, r, m.value) IN
    /\ m \in messages /\ m.kind = "bus" /\ m.dst = r
    /\ ScheduleActive(r)
    /\ BusOwner(m.value) \in registered[r]
    /\ s \in Slots /\ memLog[r][s] = Empty
    /\ memLog' = [memLog EXCEPT ![r][s] = m.value]
    /\ ownershipSeen' = [ownershipSeen EXCEPT ![s] = @ \cup {BusOwner(m.value)}]
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, connectionEpoch, canonicalLog, valueHistory,
                    executedThrough, stable, stableValue, syncVars, viewVars, diskVars,
                    clientVars, registered, scheduleAnnouncements, scheduleEpoch,
                    scheduleReady, scheduleInstallActive, scheduleAcks,
                    scheduleCommitted, releasedClients>>

\* UPDATE-MAP
\* code: paxosbus/replica.go:admitLocked
\* update: Update!InteractionActions
\* Buses reaching a lagging or view-changing replica are observed for resume
\* fencing by the implementation but are not retained for later admission.
RejectFencedBus(r, m) ==
    /\ m \in messages /\ m.kind = "bus" /\ m.dst = r
    /\ ~ScheduleActive(r)
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, connectionEpoch, logVars, syncVars, viewVars,
                    diskVars, clientVars, scheduleVars>>

\* replica.go:1089-1101,1156-1163,1201-1227: execute, dedup, reply, enqueue disk write.
ExecuteSlot(r, s) ==
    LET v == memLog[r][s]
        idx == nextIndex[r]
    IN
    /\ replicaStatus[r] = "normal" /\ s = executedThrough[r] + 1
    /\ v \in StoredValues
    /\ executedThrough' = [executedThrough EXCEPT ![r] = s]
    /\ canonicalLog' = [canonicalLog EXCEPT ![s] = v]
    /\ valueHistory' = [valueHistory EXCEPT ![s] = @ \cup {v}]
    /\ canonicalGeneration' = [canonicalGeneration EXCEPT ![s] =
                                  IF canonicalLog[s] = v THEN @ ELSE @ + 1]
    /\ IF r \in DurableReplicas /\ diskLog[r][s] = Empty
          THEN /\ diskLog' = [diskLog EXCEPT ![r][s] = v]
               /\ diskGeneration' = [diskGeneration EXCEPT ![r][s] = canonicalGeneration'[s]]
          ELSE UNCHANGED <<diskLog, diskGeneration>>
    /\ IF v \in RealValues
          THEN /\ nextIndex' = [nextIndex EXCEPT ![r] = @ + 1]
               /\ generatedReplies' = [generatedReplies EXCEPT ![v] = @ \cup {r}]
               /\ messages' = messages \cup
                    {Msg("reply", r, BusOwner(v), view[r], s, v, 0)}
               /\ IF dedupIndex[r][v] = 0
                     THEN /\ dedupIndex' = [dedupIndex EXCEPT ![r][v] = idx]
                          /\ requestIndexes' = [requestIndexes EXCEPT ![v] = @ \cup {idx}]
                          /\ executionCount' = [executionCount EXCEPT ![r][v] = @ + 1]
                     ELSE UNCHANGED <<dedupIndex, requestIndexes, executionCount>>
          ELSE UNCHANGED <<nextIndex, generatedReplies, messages, dedupIndex,
                           requestIndexes, executionCount>>
    /\ UNCHANGED <<connectionUp, connectionEpoch, memLog, stable, stableValue,
                    syncVars, viewVars, pruned, transferValue, transferFromDisk,
                    deliveredReplies,
                    committedValues, pendingReboard, scheduleVars>>

\* client.go:494-520,530+: receive a generated reply and count its replica vote.
ClientHandlesReply(m) ==
    /\ m \in messages /\ m.kind = "reply" /\ m.value \in RealValues
    /\ deliveredReplies' = [deliveredReplies EXCEPT ![m.value] = @ \cup {m.src}]
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, connectionEpoch, logVars, syncVars, viewVars,
                    diskVars, generatedReplies, committedValues, dedupIndex,
                    requestIndexes, executionCount, nextIndex, pendingReboard, scheduleVars>>

\* client.go:518-520: commitment needs a quorum including the view leader.
ClientCommits(v) ==
    /\ v \notin committedValues
    /\ Quorum(deliveredReplies[v]) /\ Leader(0) \in deliveredReplies[v]
    /\ committedValues' = committedValues \cup {v}
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskVars,
                    generatedReplies, deliveredReplies, dedupIndex, requestIndexes,
                    executionCount, nextIndex, pendingReboard, scheduleVars>>

\* recovery.go:52-81 and 1490-1515: leader initiates a prefix proof.
SendSyncPrepare(r, q, s) ==
    /\ r = Leader(view[r]) /\ replicaStatus[r] = "normal"
    /\ q # r /\ connectionUp[r][q] /\ s \in 1..executedThrough[r]
    /\ Send(Msg("sync-prepare", r, q, view[r], s, memLog[r][s], connectionEpoch[r][q]))
    /\ syncAcks' = [syncAcks EXCEPT ![r][s] = {r}]
    /\ prepareDisposition' = [prepareDisposition EXCEPT ![r][s] = "match"]
    /\ prepareValue' = [prepareValue EXCEPT ![r][s] = memLog[r][s]]
    /\ UNCHANGED <<connectionUp, connectionEpoch, logVars, viewVars, diskVars,
                    clientVars, scheduleVars>>

\* recovery.go:140-197,236-257: compare prefix; no-stable mismatch returns without repair.
HandleSyncPrepare(r, m) ==
    LET d == IF m.slot \in pruned[r] THEN "unverified"
             ELSE IF memLog[r][m.slot] = m.value THEN "match" ELSE "mismatch"
    IN
    /\ m \in messages /\ m.kind = "sync-prepare" /\ m.dst = r
    /\ m.view = view[r] /\ m.src = Leader(view[r])
    /\ prepareDisposition' = [prepareDisposition EXCEPT ![r][m.slot] = d]
    /\ prepareValue' = [prepareValue EXCEPT ![r][m.slot] = m.value]
    /\ IF d \in {"match", "unverified"}
          THEN messages' = (messages \ {m}) \cup
                 {Msg("sync-reply", r, m.src, m.view, m.slot, m.value, m.epoch)}
          ELSE messages' = messages \ {m}
    /\ UNCHANGED <<connectionUp, connectionEpoch, logVars, syncAcks, viewVars,
                    diskVars, clientVars, scheduleVars>>

\* recovery.go:200-214: accumulate replies for the exact view/slot prepare.
HandleSyncReply(r, m) ==
    /\ m \in messages /\ m.kind = "sync-reply" /\ m.dst = r
    /\ r = Leader(m.view) /\ m.view = view[r]
    /\ syncAcks' = [syncAcks EXCEPT ![r][m.slot] = @ \cup {m.src}]
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, connectionEpoch, logVars, prepareDisposition,
                    prepareValue, viewVars, diskVars, clientVars, scheduleVars>>

\* recovery.go:204-214: quorum decision broadcasts commit without a hash (S1/E1).
MaybeCommitSync(r, s) ==
    /\ r = Leader(view[r]) /\ Quorum(syncAcks[r][s])
    /\ messages' = messages \cup UNION
         {IF connectionUp[r][q]
             THEN {Msg("sync-commit", r, q, view[r], s, Empty, connectionEpoch[r][q])}
             ELSE {} : q \in Replicas \ {r}}
    /\ stable' = [stable EXCEPT ![r] = s]
    /\ stableValue' = [stableValue EXCEPT ![r][s] = memLog[r][s]]
    /\ UNCHANGED <<connectionUp, connectionEpoch, memLog, canonicalLog, valueHistory,
                    executedThrough, prepareDisposition, prepareValue, syncAcks,
                    viewVars, diskVars, clientVars, scheduleVars>>

\* recovery.go:222-233: follower accepts leader/view commit with no prepare binding.
HandleSyncCommit(r, m) ==
    /\ m \in messages /\ m.kind = "sync-commit" /\ m.dst = r
    /\ m.view = view[r] /\ m.src = Leader(view[r])
    /\ m.slot <= executedThrough[r] /\ m.slot > stable[r]
    /\ stable' = [stable EXCEPT ![r] = m.slot]
    /\ stableValue' = [stableValue EXCEPT ![r][m.slot] = memLog[r][m.slot]]
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, connectionEpoch, memLog, canonicalLog, valueHistory,
                    executedThrough, syncVars, viewVars, diskVars, clientVars, scheduleVars>>

\* replica.go:1467-1480: a failed connection skips sends; replacement has a new epoch.
Disconnect(a, b) ==
    /\ a # b /\ connectionUp[a][b]
    /\ connectionUp' = [connectionUp EXCEPT ![a][b] = FALSE]
    /\ UNCHANGED <<messages, connectionEpoch, logVars, syncVars, viewVars,
                    diskVars, clientVars, scheduleVars>>

Reconnect(a, b) ==
    /\ a # b /\ ~connectionUp[a][b]
    /\ connectionUp' = [connectionUp EXCEPT ![a][b] = TRUE]
    /\ connectionEpoch' = [connectionEpoch EXCEPT ![a][b] = @ + 1]
    /\ UNCHANGED <<messages, logVars, syncVars, viewVars, diskVars,
                    clientVars, scheduleVars>>

\* send/decoder paths: crash-fault network may lose any in-flight message.
LoseMessage(m) ==
    /\ m \in messages
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, connectionEpoch, logVars, syncVars, viewVars,
                    diskVars, clientVars, scheduleVars>>

\* UPDATE-MAP
\* code: paxosbus/recovery.go:beginViewChangeLocked
\* update: Update!AffectedActions
\* Entering a new view fences bus admission and abandons any incomplete
\* collection/install attempt, while retaining the last complete schedule for
\* the view-change report.
BeginViewChange(r) ==
    /\ view[r] < MaxView
    /\ view' = [view EXCEPT ![r] = @ + 1]
    /\ replicaStatus' = [replicaStatus EXCEPT ![r] = "view-change"]
    /\ reportReceived' = [reportReceived EXCEPT ![Leader(view'[r])] = {}]
    /\ scheduleReady' = [scheduleReady EXCEPT ![r] = FALSE]
    /\ scheduleAnnouncements' = [scheduleAnnouncements EXCEPT ![r] = {}]
    /\ scheduleInstallActive' = [scheduleInstallActive EXCEPT ![r] = FALSE]
    /\ scheduleAcks' = [scheduleAcks EXCEPT ![r] = {}]
    /\ UNCHANGED <<networkVars, logVars, syncVars, reportEvidence, reportSchedule,
                    chosenDonor, donorAvailable, fetchAttempts, diskVars, clientVars,
                    registered, ownershipSeen, scheduleEpoch, scheduleCommitted,
                    releasedClients>>

ReportBase(r) == IF executedThrough[r] + 1 > MetadataWindow
                 THEN executedThrough[r] - MetadataWindow + 1 ELSE 1
ReportOf(r, s) == IF s < ReportBase(r) THEN "unknown"
                  ELSE IF memLog[r][s] # Empty THEN "present" ELSE "absent"

\* UPDATE-MAP
\* code: paxosbus/recovery.go:buildViewChangeLocked,mergeSuffix
\* update: Update!AffectedActions
\* The report carries either the complete authoritative schedule or no schedule;
\* partial schedules are never installed by the new implementation.
BuildViewChangeReport(r, l) ==
    LET ev == [s \in Slots |-> ReportOf(r, s)] IN
    /\ l = Leader(view[r]) /\ replicaStatus[r] = "view-change"
    /\ reportReceived' = [reportReceived EXCEPT ![l] = @ \cup {r}]
    /\ reportEvidence' = [reportEvidence EXCEPT ![l][r] = ev]
    /\ reportSchedule' = [reportSchedule EXCEPT ![l][r] = registered[r]]
    /\ chosenDonor' = [chosenDonor EXCEPT ![l] =
          [s \in Slots |-> IF @[s] = NoReplica /\ ev[s] = "present" THEN r ELSE @[s]]]
    /\ UNCHANGED <<networkVars, logVars, syncVars, view, replicaStatus,
                    donorAvailable, fetchAttempts, diskVars, clientVars, scheduleVars>>

\* UPDATE-MAP
\* code: paxosbus/recovery.go:driveViewChange,installStartView
\* update: Update!AffectedActions
\* Quorum intersection ensures that a previously committed complete schedule is
\* present in the selected reports. Empty reports permit an initial view change
\* before any schedule committed, after which the new leader recollects clients.
InstallViewSchedule(r, l) ==
    /\ replicaStatus[r] = "view-change" /\ l = Leader(view[r])
    /\ view[l] = view[r] /\ Quorum(reportReceived[l])
    /\ LET selected == UNION {reportSchedule[l][q] : q \in reportReceived[l]} IN
       /\ selected \in {{}, Clients}
       /\ registered' = [registered EXCEPT ![r] = selected]
       /\ scheduleEpoch' = [scheduleEpoch EXCEPT ![r] = view[r]]
       /\ scheduleReady' = [scheduleReady EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskVars, clientVars,
                    ownershipSeen, scheduleAnnouncements, scheduleInstallActive,
                    scheduleAcks, scheduleCommitted, releasedClients>>

\* recovery.go:861-890: retry only the remembered first donor for a slot.
FetchMergedSlot(l, s) ==
    LET d == chosenDonor[l][s] IN
    /\ d \in Replicas /\ fetchAttempts[l][s] < MaxFetchAttempts
    /\ fetchAttempts' = [fetchAttempts EXCEPT ![l][s] = @ + 1]
    /\ IF donorAvailable[d] /\ memLog[d][s] # Empty
          THEN memLog' = [memLog EXCEPT ![l][s] = memLog[d][s]]
          ELSE UNCHANGED memLog
    /\ UNCHANGED <<networkVars, canonicalLog, valueHistory, executedThrough, stable,
                    stableValue, syncVars, view, replicaStatus, reportReceived,
                    reportEvidence, reportSchedule, chosenDonor, donorAvailable, diskVars,
                    clientVars, scheduleVars>>

\* recovery.go:892-907 and 663-677: failed/unknown evidence becomes canonical no-op.
CloseMergedSlotAsNoOp(l, s) ==
    /\ replicaStatus[l] = "view-change" /\ Quorum(reportReceived[l])
    /\ memLog[l][s] = Empty
    \* The leader fixes the canonical plan before broadcasting StartView. Once
    \* any replica has completed this view, the plan cannot change underneath it.
    /\ \/ canonicalLog[s] = NoOp
       \/ \A q \in Replicas : view[q] = view[l] => replicaStatus[q] = "view-change"
    /\ \/ /\ chosenDonor[l][s] \in Replicas
           /\ fetchAttempts[l][s] = MaxFetchAttempts
       \/ /\ chosenDonor[l][s] = NoReplica
           /\ \A r \in reportReceived[l] : reportEvidence[l][r][s] # "present"
    /\ memLog' = [memLog EXCEPT ![l][s] = NoOp]
    /\ canonicalLog' = [canonicalLog EXCEPT ![s] = NoOp]
    /\ valueHistory' = [valueHistory EXCEPT ![s] = @ \cup {NoOp}]
    /\ canonicalGeneration' = [canonicalGeneration EXCEPT ![s] = @ + 1]
    /\ UNCHANGED <<networkVars, executedThrough, stable, stableValue, syncVars,
                    viewVars, diskLog, diskGeneration, pruned, transferValue,
                    transferFromDisk,
                    clientVars, scheduleVars>>

\* recovery.go:1382-1451,1490-1515: rewind then install different canonical contents.
InstallCanonicalSlot(r, s, v) ==
    /\ replicaStatus[r] = "view-change" /\ v \in StoredValues
    /\ memLog[r][s] # v
    /\ \/ canonicalLog[s] = v
       \/ \A q \in Replicas : view[q] = view[r] => replicaStatus[q] = "view-change"
    \* recovery.go:614-677,820-907: install only selected donor data or a planned no-op.
    /\ \/ canonicalLog[s] = v
       \/ /\ \E d \in Replicas :
                 /\ reportEvidence[r][d][s] = "present"
                 /\ memLog[d][s] = v
       \/ /\ v = NoOp
           /\ Quorum(reportReceived[r])
           /\ chosenDonor[r][s] = NoReplica
    /\ memLog' = [memLog EXCEPT ![r][s] = v]
    /\ canonicalLog' = [canonicalLog EXCEPT ![s] = v]
    /\ valueHistory' = [valueHistory EXCEPT ![s] = @ \cup {v}]
    /\ canonicalGeneration' = [canonicalGeneration EXCEPT ![s] = @ + 1]
    /\ IF s <= executedThrough[r]
          THEN executedThrough' = [executedThrough EXCEPT ![r] = s - 1]
          ELSE UNCHANGED executedThrough
    /\ UNCHANGED <<networkVars, stable, stableValue, syncVars, viewVars,
                    diskLog, diskGeneration, pruned, transferValue,
                    transferFromDisk,
                    clientVars, scheduleVars>>

\* recovery.go:762-781,1351-1407: the leader fixes the canonical recovery
\* result before broadcasting StartView. A follower may process that broadcast
\* before the leader publishes normal, but the plan is already immutable. Every
\* replica verifies that reported-present slots have been recovered and that
\* canonical slots match locally before publishing normal.
RecoveryComplete(r) ==
    LET l == Leader(view[r]) IN
    /\ Quorum(reportReceived[l])
    /\ \A s \in Slots :
         /\ ((\E q \in reportReceived[l] : reportEvidence[l][q][s] = "present")
               => memLog[r][s] # Empty)
         /\ (canonicalLog[s] # Empty => memLog[r][s] = canonicalLog[s])
    /\ \A s \in 1..stable[r] : memLog[r][s] = stableValue[r][s]

\* UPDATE-MAP
\* code: paxosbus/recovery.go:publishNormalViewLocked,finishRecoveryIfComplete
\* update: Update!AffectedActions
\* Publishing the view re-enables bus admission only after canonical recovery
\* and prefix verification complete. An empty schedule remains fenced for
\* recollection.
FinishViewChange(r) ==
    /\ replicaStatus[r] = "view-change"
    /\ scheduleEpoch[r] = view[r]
    /\ RecoveryComplete(r)
    /\ replicaStatus' = [replicaStatus EXCEPT ![r] = "normal"]
    /\ scheduleReady' = [scheduleReady EXCEPT ![r] = registered[r] = Clients]
    /\ UNCHANGED <<networkVars, logVars, syncVars, view, reportReceived,
                    reportEvidence, reportSchedule, chosenDonor, donorAvailable,
                    fetchAttempts, diskVars, clientVars, registered, ownershipSeen,
                    scheduleAnnouncements, scheduleEpoch, scheduleInstallActive,
                    scheduleAcks, scheduleCommitted, releasedClients>>

\* recovery.go:820-907: donor failure is environmental and may recover later.
SetDonorAvailable(r, b) ==
    /\ donorAvailable[r] # b
    /\ donorAvailable' = [donorAvailable EXCEPT ![r] = b]
    /\ UNCHANGED <<networkVars, logVars, syncVars, view, replicaStatus,
                    reportReceived, reportEvidence, reportSchedule, chosenDonor, fetchAttempts,
                    diskVars, clientVars, scheduleVars>>

\* replica.go:1106-1154: stable retention prunes memory and its dedup entry (S4/E5/E6).
PruneCommittedSlot(r, s) ==
    LET v == memLog[r][s] IN
    /\ s <= stable[r] /\ s \notin pruned[r] /\ s + RetainWindow < executedThrough[r]
    /\ memLog' = [memLog EXCEPT ![r][s] = Empty]
    /\ pruned' = [pruned EXCEPT ![r] = @ \cup {s}]
    /\ IF v \in RealValues
          THEN dedupIndex' = [dedupIndex EXCEPT ![r][v] = 0]
          ELSE UNCHANGED dedupIndex
    /\ UNCHANGED <<networkVars, canonicalLog, valueHistory, executedThrough,
                    stable, stableValue, syncVars, viewVars, diskLog, diskGeneration,
                    canonicalGeneration, transferValue, transferFromDisk,
                    generatedReplies,
                    deliveredReplies, committedValues, requestIndexes, executionCount,
                    nextIndex, pendingReboard, scheduleVars>>

\* client.go:460-491: an unobserved request is reboarded after timeout.
ClientReboards(v) ==
    /\ v \in RealValues /\ v \notin committedValues
    \* A timeout retry can only reboard a request that was previously executed;
    \* generated-but-undelivered replies are the safety-relevant case.
    /\ generatedReplies[v] # {}
    /\ pendingReboard' = pendingReboard \cup {v}
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskVars,
                    generatedReplies, deliveredReplies, committedValues, dedupIndex,
                    requestIndexes, executionCount, nextIndex, scheduleVars>>

\* replica.go:847-868,993-999: retry may be assigned into a later empty slot.
HandleReboardedBus(r, v, s) ==
    \* The retry is carried by the owner's next bus.  With a fixed schedule this
    \* is one complete client round after the original bus, not an arbitrary slot.
    /\ ScheduleActive(r)
    /\ BusOwner(v) \in registered[r]
    /\ s = ScheduledSlot(registered, r, v) + Cardinality(registered[r])
    /\ v \in pendingReboard /\ s \in Slots /\ memLog[r][s] = Empty
    /\ s > stable[r]
    /\ memLog' = [memLog EXCEPT ![r][s] = v]
    /\ pendingReboard' = pendingReboard \ {v}
    /\ ownershipSeen' = [ownershipSeen EXCEPT ![s] = @ \cup {BusOwner(v)}]
    /\ UNCHANGED <<networkVars, canonicalLog, valueHistory, executedThrough,
                    stable, stableValue, syncVars, viewVars, diskVars,
                    generatedReplies, deliveredReplies, committedValues, dedupIndex,
                    requestIndexes, executionCount, nextIndex, registered,
                    scheduleAnnouncements, scheduleEpoch, scheduleReady,
                    scheduleInstallActive, scheduleAcks, scheduleCommitted,
                    releasedClients>>

\* recovery.go:1969-2032 and logreader.go:110-147: memory first, then stable disk.
ServeState(from, to, s) ==
    /\ s \in Slots
    /\ to # from
    /\ IF memLog[from][s] # Empty
          THEN /\ transferValue' = [transferValue EXCEPT ![to][s] = memLog[from][s]]
               /\ transferFromDisk' = [transferFromDisk EXCEPT ![to][s] = FALSE]
          ELSE /\ s \in pruned[from] /\ s <= stable[from]
               /\ diskLog[from][s] # Empty
               /\ transferValue' = [transferValue EXCEPT ![to][s] = diskLog[from][s]]
               /\ transferFromDisk' = [transferFromDisk EXCEPT ![to][s] = TRUE]
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskLog,
                    diskGeneration, canonicalGeneration, pruned, clientVars, scheduleVars>>

\* ----------------------------- Invariants -----------------------------
TypeOK ==
    /\ messages \subseteq Message
    /\ connectionUp \in [Replicas -> [Replicas -> BOOLEAN]]
    /\ connectionEpoch \in [Replicas -> [Replicas -> Nat]]
    /\ memLog \in [Replicas -> [Slots -> LogValues]]
    /\ canonicalLog \in [Slots -> LogValues]
    /\ valueHistory \in [Slots -> SUBSET StoredValues]
    /\ executedThrough \in [Replicas -> 0..MaxSlot]
    /\ stable \in [Replicas -> 0..MaxSlot]
    /\ stableValue \in [Replicas -> [Slots -> LogValues]]
    /\ view \in [Replicas -> Views] /\ replicaStatus \in [Replicas -> Status]
    /\ prepareDisposition \in [Replicas -> [Slots -> Disposition]]
    /\ prepareValue \in [Replicas -> [Slots -> LogValues]]
    /\ syncAcks \in [Replicas -> [Slots -> SUBSET Replicas]]
    /\ reportReceived \in [Replicas -> SUBSET Replicas]
    /\ reportEvidence \in [Replicas -> [Replicas -> [Slots -> Evidence]]]
    /\ reportSchedule \in [Replicas -> [Replicas -> SUBSET Clients]]
    /\ chosenDonor \in [Replicas -> [Slots -> Replicas \cup {NoReplica}]]
    /\ donorAvailable \in [Replicas -> BOOLEAN]
    /\ fetchAttempts \in [Replicas -> [Slots -> 0..MaxFetchAttempts]]
    /\ diskLog \in [Replicas -> [Slots -> LogValues]]
    /\ diskGeneration \in [Replicas -> [Slots -> Nat]]
    /\ canonicalGeneration \in [Slots -> Nat]
    /\ pruned \in [Replicas -> SUBSET Slots]
    /\ transferValue \in [Replicas -> [Slots -> LogValues]]
    /\ transferFromDisk \in [Replicas -> [Slots -> BOOLEAN]]
    /\ generatedReplies \in [RealValues -> SUBSET Replicas]
    /\ deliveredReplies \in [RealValues -> SUBSET Replicas]
    /\ committedValues \subseteq RealValues
    /\ dedupIndex \in [Replicas -> [RealValues -> Nat]]
    /\ requestIndexes \in [RealValues -> SUBSET Nat]
    /\ executionCount \in [Replicas -> [RealValues -> Nat]]
    /\ nextIndex \in [Replicas -> Nat]
    /\ pendingReboard \subseteq RealValues
    /\ registered \in [Replicas -> SUBSET Clients]
    /\ ownershipSeen \in [Slots -> SUBSET Clients]
    /\ scheduleAnnouncements \in [Replicas -> SUBSET Clients]
    /\ scheduleEpoch \in [Replicas -> Views]
    /\ scheduleReady \in [Replicas -> BOOLEAN]
    /\ scheduleInstallActive \in [Replicas -> BOOLEAN]
    /\ scheduleAcks \in [Replicas -> SUBSET Replicas]
    /\ scheduleCommitted \in BOOLEAN
    /\ releasedClients \subseteq Clients

ExecutedPrefixBounded == \A r \in Replicas : stable[r] <= executedThrough[r]
DiskWriteOnce == \A r \in Replicas, s \in Slots :
    diskGeneration[r][s] <= canonicalGeneration[s]

SlotAgreement == \A s \in Slots : Cardinality(valueHistory[s] \cap RealValues) <= 1
StablePrefixesAgree ==
    \A r \in Replicas, q \in Replicas :
      \A s \in 1..Min({stable[r], stable[q]}) : stableValue[r][s] = stableValue[q][s]
Durability ==
    \A v \in committedValues : \E s \in Slots : canonicalLog[s] = v
NoOpLogDurability ==
    \A s \in Slots : ~(NoOp \in valueHistory[s] /\ (valueHistory[s] \cap RealValues) # {})
Linearizability ==
    \A v \in RealValues, w \in RealValues :
      v # w => requestIndexes[v] \cap requestIndexes[w] = {}

CommittedRequestSurvivesViewChange ==
    \A v \in committedValues : \E s \in Slots : canonicalLog[s] = v

Authoritative(r, s) ==
    memLog[r][s] = canonicalLog[s] \/
    (s \in pruned[r] /\ diskLog[r][s] = canonicalLog[s] /\
     diskGeneration[r][s] = canonicalGeneration[s])
CanonicalDataHasRecoverableSource ==
    \A s \in Slots :
      (canonicalLog[s] # Empty /\ \E r \in Replicas : s <= stable[r]) =>
        \E r \in Replicas : Authoritative(r, s)
DurableStateMatchesCanonical ==
    \A r \in Replicas, s \in Slots :
      transferValue[r][s] # Empty /\ transferFromDisk[r][s] =>
        transferValue[r][s] = canonicalLog[s]
RequestExecutionUnique ==
    \A r \in Replicas, v \in RealValues : executionCount[r][v] <= 1
RequestIndexStable == \A v \in RealValues : Cardinality(requestIndexes[v]) <= 1
ScheduleOwnershipStable == \A s \in Slots : Cardinality(ownershipSeen[s]) <= 1
ReadyReplicaHasCompleteSchedule ==
    \A r \in Replicas : scheduleReady[r] => ScheduleActive(r)
ReleasedOnlyAfterScheduleCommit == releasedClients # {} => scheduleCommitted
NormalViewPreservesCommittedSchedule ==
    scheduleCommitted =>
      \A r \in Replicas :
        (replicaStatus[r] = "normal" /\ view[r] > 0) => ScheduleActive(r)

Next ==
    \/ \E r \in Replicas, c \in Clients : ClientAnnouncesSchedule(c, r)
    \/ \E r \in Replicas, m \in messages : HandleScheduleAnnouncement(r, m)
    \/ \E l, r \in Replicas : SendScheduleInstall(l, r)
    \/ \E r \in Replicas, m \in messages : HandleScheduleInstall(r, m)
    \/ \E l \in Replicas, m \in messages : HandleScheduleAck(l, m)
    \/ \E l \in Replicas : CommitSchedule(l)
    \/ \E c \in Clients, v \in RealValues, r \in Replicas : ClientSendsBus(c, v, r)
    \/ \E r \in Replicas, m \in messages : HandleBus(r, m)
    \/ \E r \in Replicas, m \in messages : RejectFencedBus(r, m)
    \/ \E r \in Replicas, s \in Slots : ExecuteSlot(r, s)
    \/ \E m \in messages : ClientHandlesReply(m)
    \/ \E v \in RealValues : ClientCommits(v)
    \/ \E r, q \in Replicas, s \in Slots : SendSyncPrepare(r, q, s)
    \/ \E r \in Replicas, m \in messages : HandleSyncPrepare(r, m)
    \/ \E r \in Replicas, m \in messages : HandleSyncReply(r, m)
    \/ \E r \in Replicas, s \in Slots : MaybeCommitSync(r, s)
    \/ \E r \in Replicas, m \in messages : HandleSyncCommit(r, m)
    \/ \E a, b \in Replicas : Disconnect(a, b) \/ Reconnect(a, b)
    \/ \E m \in messages : LoseMessage(m)
    \/ \E r \in Replicas : BeginViewChange(r)
    \/ \E r, l \in Replicas : BuildViewChangeReport(r, l)
    \/ \E r, l \in Replicas : InstallViewSchedule(r, l)
    \/ \E l \in Replicas, s \in Slots : FetchMergedSlot(l, s)
    \/ \E l \in Replicas, s \in Slots : CloseMergedSlotAsNoOp(l, s)
    \/ \E r \in Replicas, s \in Slots, v \in StoredValues : InstallCanonicalSlot(r, s, v)
    \/ \E r \in Replicas : FinishViewChange(r)
    \/ \E r \in Replicas, b \in BOOLEAN : SetDonorAvailable(r, b)
    \/ \E r \in Replicas, s \in Slots : PruneCommittedSlot(r, s)
    \/ \E v \in RealValues : ClientReboards(v)
    \/ \E r \in Replicas, v \in RealValues, s \in Slots : HandleReboardedBus(r, v, s)
    \/ \E a, b \in Replicas, s \in Slots : ServeState(a, b, s)

ReplicaOrderValue == <<r0, r1, r2>>
ClientOrderValue == <<c0, c1>>

Spec == Init /\ [][Next]_vars
=============================================================================
