------------------------------ MODULE base ------------------------------
\* PaxosBus @ 07d9b4e. Category A (distributed/message passing).
\* Scope is driven by modeling-brief Scenarios 1-5. Hashes, timestamps and
\* payload bytes are represented by immutable symbolic values.

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Replicas, ReplicaOrder, Clients, ClientOrder,
          r0, r1, r2, c0, c1, BusA, BusB, NoOp, Empty, NoReplica,
          MaxSlot, MaxView, MaxWindow, MetadataWindow, MaxFetchAttempts,
          RetainWindow, DurableReplicas

ASSUME /\ IsFiniteSet(Replicas) /\ Cardinality(Replicas) = 3
       /\ ReplicaOrder \in Seq(Replicas) /\ Len(ReplicaOrder) = 3
       /\ IsFiniteSet(Clients) /\ Cardinality(Clients) = 2
       /\ ClientOrder \in Seq(Clients) /\ Len(ClientOrder) = 2
       /\ MaxSlot \in Nat \ {0} /\ MaxView \in Nat
       /\ MaxWindow \in Nat \ {0} /\ MetadataWindow \in Nat \ {0}
       /\ MaxFetchAttempts \in Nat \ {0} /\ RetainWindow \in Nat
       /\ DurableReplicas \subseteq Replicas

Slots == 1..MaxSlot
Views == 0..MaxView
Windows == 1..MaxWindow
RealValues == {BusA, BusB}
LogValues == RealValues \cup {NoOp, Empty}
StoredValues == LogValues \ {Empty}
Status == {"normal", "view-change"}
Evidence == {"present", "absent", "unknown"}
Disposition == {"none", "match", "mismatch", "unverified"}
MessageKinds == {"bus", "reply", "sync-prepare", "sync-reply", "sync-commit",
                  "view-report", "schedule-prepare", "schedule-reply",
                  "schedule-commit", "control-status"}

Quorum(S) == Cardinality(S) * 2 > Cardinality(Replicas)
Leader(v) == ReplicaOrder[(v % Len(ReplicaOrder)) + 1]
Rank(c) == CHOOSE i \in 1..Len(ClientOrder) : ClientOrder[i] = c
BusOwner(x) == IF x = BusA THEN ClientOrder[1] ELSE ClientOrder[2]
Min2(a, b) == IF a <= b THEN a ELSE b
MaxWindowOf(S) == CHOOSE w \in S : \A z \in S : z <= w

\* schedule.go:240-270 and schedule_transport.go:56-82 (Scenario 5).
\* A window may contain only a subset of clients; interval 0 creates no slot.
EnabledClients(members, intervals) == {c \in members : intervals[c] = 1}
WindowOwner(members, intervals, ordinal) ==
    CHOOSE c \in EnabledClients(members, intervals) :
      Cardinality({d \in EnabledClients(members, intervals) : Rank(d) <= Rank(c)}) = ordinal

Msg(k, a, b, vw, sl, x, ep, win, members, intervals) ==
    [kind |-> k, src |-> a, dst |-> b, view |-> vw, slot |-> sl,
     value |-> x, epoch |-> ep, window |-> win, members |-> members,
     intervals |-> intervals]

Message == [kind : MessageKinds, src : Replicas \cup Clients,
            dst : Replicas \cup Clients, view : Views, slot : 0..MaxSlot,
            value : LogValues, epoch : Nat, window : 0..MaxWindow,
            members : SUBSET Clients, intervals : [Clients -> 0..1]]

VARIABLES
    messages, connectionUp,
    memLog, canonicalLog, valueHistory, executedThrough,
    stableSlot, stableValue, prepareDisposition, prepareValue, syncAcks, activeSyncSlot,
    syncCertificates,
    view, replicaStatus, reportReceived, reportEvidence, chosenDonor,
    donorAvailable, fetchAttempts,
    diskLog, diskGeneration, canonicalGeneration, pruned,
    transferValue, transferFromDisk, transferGeneration,
    deliveredReplies, clientCommitted,
    scheduleHistory, windowMembers, windowIntervals, preparedWindow,
    preparedAcks, committedEnd, scheduleEpoch, clientAuthorizedWindow,
    ownershipSeen,
    controlReports, clientPauseView, clientActiveView, clientPaused,
    controlConnUp, dataConnUp

networkVars == <<messages, connectionUp>>
logVars == <<memLog, canonicalLog, valueHistory, executedThrough,
             stableSlot, stableValue>>
syncVars == <<prepareDisposition, prepareValue, syncAcks, activeSyncSlot, syncCertificates>>
viewVars == <<view, replicaStatus, reportReceived, reportEvidence,
              chosenDonor, donorAvailable, fetchAttempts>>
diskVars == <<diskLog, diskGeneration, canonicalGeneration, pruned,
              transferValue, transferFromDisk, transferGeneration>>
clientVars == <<deliveredReplies, clientCommitted, controlReports,
                clientPauseView, clientActiveView, clientPaused,
                controlConnUp, dataConnUp>>
scheduleVars == <<scheduleHistory, windowMembers, windowIntervals,
                  preparedWindow, preparedAcks, committedEnd, scheduleEpoch,
                  clientAuthorizedWindow, ownershipSeen>>
vars == <<networkVars, logVars, syncVars, viewVars, diskVars,
          clientVars, scheduleVars>>

EmptyIntervals == [c \in Clients |-> 0]

Init ==
    /\ messages = {}
    /\ connectionUp = [a \in Replicas |-> [b \in Replicas |-> a # b]]
    /\ memLog = [r \in Replicas |-> [s \in Slots |-> Empty]]
    /\ canonicalLog = [s \in Slots |-> Empty]
    /\ valueHistory = [s \in Slots |-> {}]
    /\ executedThrough = [r \in Replicas |-> 0]
    /\ stableSlot = [r \in Replicas |-> 0]
    /\ stableValue = [r \in Replicas |-> [s \in Slots |-> Empty]]
    /\ prepareDisposition = [r \in Replicas |-> [s \in Slots |-> "none"]]
    /\ prepareValue = [r \in Replicas |-> [s \in Slots |-> Empty]]
    /\ syncAcks = [r \in Replicas |-> [s \in Slots |-> {}]]
    /\ activeSyncSlot = [r \in Replicas |-> 0]
    /\ syncCertificates = {}
    /\ view = [r \in Replicas |-> 0]
    /\ replicaStatus = [r \in Replicas |-> "normal"]
    /\ reportReceived = [r \in Replicas |-> {}]
    /\ reportEvidence = [l \in Replicas |-> [r \in Replicas |-> [s \in Slots |-> "unknown"]]]
    /\ chosenDonor = [l \in Replicas |-> [s \in Slots |-> NoReplica]]
    /\ donorAvailable = [r \in Replicas |-> TRUE]
    /\ fetchAttempts = [r \in Replicas |-> [s \in Slots |-> 0]]
    /\ diskLog = [r \in Replicas |-> [s \in Slots |-> Empty]]
    /\ diskGeneration = [r \in Replicas |-> [s \in Slots |-> 0]]
    /\ canonicalGeneration = [s \in Slots |-> 0]
    /\ pruned = [r \in Replicas |-> {}]
    /\ transferValue = [r \in Replicas |-> [s \in Slots |-> Empty]]
    /\ transferFromDisk = [r \in Replicas |-> [s \in Slots |-> FALSE]]
    /\ transferGeneration = [r \in Replicas |-> [s \in Slots |-> 0]]
    /\ deliveredReplies = [x \in RealValues |-> {}]
    /\ clientCommitted = {}
    /\ scheduleHistory = [r \in Replicas |-> {}]
    /\ windowMembers = [r \in Replicas |-> [w \in Windows |-> {}]]
    /\ windowIntervals = [r \in Replicas |-> [w \in Windows |-> EmptyIntervals]]
    /\ preparedWindow = [r \in Replicas |-> 0]
    /\ preparedAcks = [r \in Replicas |-> {}]
    /\ committedEnd = [r \in Replicas |-> 0]
    /\ scheduleEpoch = [r \in Replicas |-> 0]
    /\ clientAuthorizedWindow = [c \in Clients |-> 0]
    /\ ownershipSeen = [s \in Slots |-> {}]
    /\ controlReports = [c \in Clients |-> [r \in Replicas |-> [view |-> 0, normal |-> TRUE]]]
    /\ clientPauseView = [c \in Clients |-> 0]
    /\ clientActiveView = [c \in Clients |-> 0]
    /\ clientPaused = [c \in Clients |-> FALSE]
    /\ controlConnUp = [c \in Clients |-> [r \in Replicas |-> TRUE]]
    /\ dataConnUp = [c \in Clients |-> [r \in Replicas |-> TRUE]]

Send(m) == messages' = messages \cup {m}
Consume(m) == messages' = messages \ {m}

\* schedule.go:111-145: a leader records one eligible future-window request.
RequestScheduleWindow(c, l, w, interval) ==
    /\ l = Leader(view[l]) /\ replicaStatus[l] = "normal"
    /\ w \in Windows /\ interval \in 0..1 /\ w > committedEnd[l]
    \* schedule.go:159-180 freezes the active proposal after removing its request set.
    /\ w \notin scheduleHistory[l]
    \* schedule.go:132-145: collection is per start time and client.
    /\ windowMembers' = [windowMembers EXCEPT ![l][w] = @ \cup {c}]
    /\ windowIntervals' = [windowIntervals EXCEPT ![l][w][c] = interval]
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskVars,
                    clientVars, scheduleHistory, preparedWindow, preparedAcks,
                    committedEnd, scheduleEpoch, clientAuthorizedWindow,
                    ownershipSeen>>

\* schedule.go:147-181: the leader appends a nonempty partial window and self-acks.
LeaderPrepareSchedule(l, w) ==
    /\ l = Leader(view[l]) /\ replicaStatus[l] = "normal"
    /\ preparedWindow[l] = 0 /\ w = committedEnd[l] + 1 /\ w \in Windows
    /\ windowMembers[l][w] # {}
    \* schedule.go:175-180,240-270: append prepared history before quorum commit.
    /\ scheduleHistory' = [scheduleHistory EXCEPT ![l] = @ \cup {w}]
    /\ preparedWindow' = [preparedWindow EXCEPT ![l] = w]
    /\ preparedAcks' = [preparedAcks EXCEPT ![l] = {l}]
    \* schedule_transport.go:85-99: snapshot and prepare are sent separately on the stream.
    /\ messages' = messages \cup UNION
         {IF connectionUp[l][r]
             THEN {Msg("schedule-prepare", l, r, view[l], 0, Empty, view[l], w,
                       windowMembers[l][w], windowIntervals[l][w])}
             ELSE {} : r \in Replicas \ {l}}
    /\ UNCHANGED <<connectionUp, logVars, syncVars, viewVars, diskVars,
                    clientVars, windowMembers, windowIntervals, committedEnd,
                    scheduleEpoch, clientAuthorizedWindow, ownershipSeen>>

\* schedule.go:240-279: follower validates view/leader/window and appends prepared suffix.
HandleSchedulePrepare(r, m) ==
    /\ m \in messages /\ m.kind = "schedule-prepare" /\ m.dst = r
    /\ m.view = view[r] /\ m.src = Leader(view[r]) /\ replicaStatus[r] = "normal"
    /\ m.window \in Windows /\ m.window = committedEnd[r] + 1 /\ m.members # {}
    \* schedule.go:244-270: exact partial membership and interval-zero entries are retained.
    /\ scheduleHistory' = [scheduleHistory EXCEPT ![r] = @ \cup {m.window}]
    /\ windowMembers' = [windowMembers EXCEPT ![r][m.window] = m.members]
    /\ windowIntervals' = [windowIntervals EXCEPT ![r][m.window] = m.intervals]
    /\ preparedWindow' = [preparedWindow EXCEPT ![r] = m.window]
    /\ messages' = (messages \ {m}) \cup
         {Msg("schedule-reply", r, m.src, m.view, 0, Empty, m.epoch,
              m.window, {}, EmptyIntervals)}
    /\ UNCHANGED <<connectionUp, logVars, syncVars, viewVars, diskVars,
                    clientVars, preparedAcks, committedEnd, scheduleEpoch,
                    clientAuthorizedWindow, ownershipSeen>>

\* schedule.go:281-287: only the active proposal's coordinate is acknowledged.
HandleScheduleReply(l, m) ==
    /\ m \in messages /\ m.kind = "schedule-reply" /\ m.dst = l
    /\ l = Leader(view[l]) /\ replicaStatus[l] = "normal"
    /\ m.view = view[l] /\ m.window = preparedWindow[l]
    /\ preparedAcks' = [preparedAcks EXCEPT ![l] = @ \cup {m.src}]
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, logVars, syncVars, viewVars, diskVars,
                    clientVars, scheduleHistory, windowMembers, windowIntervals,
                    preparedWindow, committedEnd, scheduleEpoch,
                    clientAuthorizedWindow, ownershipSeen>>

\* schedule.go:198-225: quorum commits the prepared window and authorizes waiters.
LeaderCommitSchedule(l) ==
    LET w == preparedWindow[l] IN
    /\ w \in Windows /\ Quorum(preparedAcks[l])
    \* schedule.go:199-205: advance committed frontier and clear proposal.
    /\ committedEnd' = [committedEnd EXCEPT ![l] = w]
    /\ preparedWindow' = [preparedWindow EXCEPT ![l] = 0]
    /\ scheduleEpoch' = [scheduleEpoch EXCEPT ![l] = view[l]]
    /\ clientAuthorizedWindow' = [c \in Clients |->
          IF c \in windowMembers[l][w] THEN w ELSE clientAuthorizedWindow[c]]
    \* schedule_transport.go:91-99: commit follows snapshot+prepare.
    /\ messages' = messages \cup UNION
         {IF connectionUp[l][r]
             THEN {Msg("schedule-commit", l, r, view[l], 0, Empty, view[l], w,
                       windowMembers[l][w], windowIntervals[l][w])}
             ELSE {} : r \in Replicas \ {l}}
    /\ UNCHANGED <<connectionUp, logVars, syncVars, viewVars, diskVars,
                    clientVars, scheduleHistory, windowMembers, windowIntervals,
                    preparedAcks, ownershipSeen>>

\* schedule.go:289-303: coordinate-only commit succeeds only if local history has the window.
HandleScheduleCommit(r, m) ==
    /\ m \in messages /\ m.kind = "schedule-commit" /\ m.dst = r
    /\ m.view = view[r] /\ m.src = Leader(view[r]) /\ replicaStatus[r] = "normal"
    /\ m.window \in scheduleHistory[r]
    /\ committedEnd' = [committedEnd EXCEPT ![r] = IF m.window > @ THEN m.window ELSE @]
    /\ scheduleEpoch' = [scheduleEpoch EXCEPT ![r] = view[r]]
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, logVars, syncVars, viewVars, diskVars,
                    clientVars, scheduleHistory, windowMembers, windowIntervals,
                    preparedWindow, preparedAcks, clientAuthorizedWindow,
                    ownershipSeen>>

ScheduleActive(r, w) ==
    /\ replicaStatus[r] = "normal" /\ scheduleEpoch[r] = view[r]
    /\ w \in scheduleHistory[r] /\ w <= committedEnd[r]

\* client.go:361-415 and replica.go:916-937: multicast a bus in an authorized window.
ClientSendsBus(c, x, r, w) ==
    /\ BusOwner(x) = c /\ w \in Windows /\ clientAuthorizedWindow[c] >= w
    /\ dataConnUp[c][r]
    /\ Send(Msg("bus", c, r, clientActiveView[c], 0, x, 0, w, {}, EmptyIntervals))
    /\ UNCHANGED <<connectionUp, logVars, syncVars, viewVars, diskVars,
                    clientVars, scheduleVars>>

\* replica.go:916-937,956+: admit only committed active schedule entries.
HandleBus(r, m) ==
    LET enabled == EnabledClients(windowMembers[r][m.window], windowIntervals[r][m.window])
        ordinal == Cardinality({c \in enabled : Rank(c) <= Rank(m.src)})
        s == m.window + ordinal - 1
    IN
    /\ m \in messages /\ m.kind = "bus" /\ m.dst = r /\ m.value \in RealValues
    /\ ScheduleActive(r, m.window) /\ m.src \in enabled
    /\ s \in Slots /\ memLog[r][s] = Empty
    /\ memLog' = [memLog EXCEPT ![r][s] = m.value]
    /\ ownershipSeen' = [ownershipSeen EXCEPT ![s] = @ \cup {m.src}]
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, canonicalLog, valueHistory, executedThrough,
                    stableSlot, stableValue, syncVars, viewVars, diskVars,
                    clientVars, scheduleHistory, windowMembers, windowIntervals,
                    preparedWindow, preparedAcks, committedEnd, scheduleEpoch,
                    clientAuthorizedWindow>>

\* replica.go:1201-1213,1313-1357: execute the next slot and persist once.
ExecuteSlot(r, s) ==
    LET x == memLog[r][s] IN
    /\ replicaStatus[r] = "normal" /\ s = executedThrough[r] + 1
    /\ x \in StoredValues
    /\ executedThrough' = [executedThrough EXCEPT ![r] = s]
    /\ canonicalLog' = [canonicalLog EXCEPT ![s] = x]
    /\ valueHistory' = [valueHistory EXCEPT ![s] = @ \cup {x}]
    /\ canonicalGeneration' = [canonicalGeneration EXCEPT ![s] =
                                  IF canonicalLog[s] = x THEN @ ELSE @ + 1]
    \* durablelog.go:225-265: an occupied old slot is not overwritten.
    /\ IF r \in DurableReplicas /\ diskLog[r][s] = Empty
          THEN /\ diskLog' = [diskLog EXCEPT ![r][s] = x]
               /\ diskGeneration' = [diskGeneration EXCEPT ![r][s] = canonicalGeneration'[s]]
          ELSE UNCHANGED <<diskLog, diskGeneration>>
    \* replica.go:1342-1357: execution queues one reply.
    /\ IF x \in RealValues
          THEN messages' = messages \cup
                 {Msg("reply", r, BusOwner(x), view[r], s, x, 0, 0, {}, EmptyIntervals)}
          ELSE UNCHANGED messages
    /\ UNCHANGED <<connectionUp, memLog, stableSlot, stableValue, syncVars,
                    viewVars, pruned, transferValue, transferFromDisk, transferGeneration,
                    deliveredReplies, clientCommitted, controlReports,
                    clientPauseView, clientActiveView, clientPaused,
                    controlConnUp, dataConnUp, scheduleVars>>

\* client.go:487-514: count a reply by (view,log index), abstracted to value+slot.
ClientHandlesReply(c, m) ==
    /\ m \in messages /\ m.kind = "reply" /\ m.dst = c /\ m.value \in RealValues
    /\ deliveredReplies' = [deliveredReplies EXCEPT ![m.value] = @ \cup {m.src}]
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, logVars, syncVars, viewVars, diskVars,
                    clientCommitted, controlReports, clientPauseView,
                    clientActiveView, clientPaused, controlConnUp, dataConnUp,
                    scheduleVars>>

\* client.go:475-478,507-514: commit requires a quorum including the view leader.
ClientCommits(c, x) ==
    /\ BusOwner(x) = c /\ x \notin clientCommitted
    /\ Quorum(deliveredReplies[x])
    /\ Leader(clientActiveView[c]) \in deliveredReplies[x]
    /\ clientCommitted' = clientCommitted \cup {x}
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskVars,
                    deliveredReplies, controlReports, clientPauseView,
                    clientActiveView, clientPaused, controlConnUp, dataConnUp,
                    scheduleVars>>

\* recovery.go:52-105: leader starts a proof round for its executed prefix.
SendSyncPrepare(l, r, s) ==
    /\ l = Leader(view[l]) /\ replicaStatus[l] = "normal"
    /\ r # l /\ connectionUp[l][r] /\ s \in 1..executedThrough[l]
    /\ activeSyncSlot[l] \in {0, s} /\ s > stableSlot[l]
    /\ messages' = messages \cup
         {Msg("sync-prepare", l, r, view[l], s, memLog[l][s], 0, 0, {}, EmptyIntervals)}
    /\ activeSyncSlot' = [activeSyncSlot EXCEPT ![l] = s]
    /\ IF activeSyncSlot[l] = 0
          THEN /\ syncAcks' = [syncAcks EXCEPT ![l][s] = {l}]
               /\ prepareDisposition' = [prepareDisposition EXCEPT ![l][s] = "match"]
               /\ prepareValue' = [prepareValue EXCEPT ![l][s] = memLog[l][s]]
          ELSE UNCHANGED <<syncAcks, prepareDisposition, prepareValue>>
    /\ UNCHANGED <<connectionUp, logVars, syncCertificates, viewVars,
                    diskVars, clientVars, scheduleVars>>

\* recovery.go:120-198: follower separately validates the prepare hash.
HandleSyncPrepare(r, m) ==
    LET d == IF m.slot \in pruned[r] THEN "unverified"
             ELSE IF memLog[r][m.slot] = m.value THEN "match" ELSE "mismatch"
    IN
    /\ m \in messages /\ m.kind = "sync-prepare" /\ m.dst = r
    /\ m.view = view[r] /\ m.src = Leader(view[r])
    /\ prepareDisposition' = [prepareDisposition EXCEPT ![r][m.slot] = d]
    /\ prepareValue' = [prepareValue EXCEPT ![r][m.slot] = m.value]
    \* recovery.go:175-198: mismatch does not reply; unverified pruned prefix may reply.
    /\ IF d \in {"match", "unverified"}
          THEN messages' = (messages \ {m}) \cup
                 {Msg("sync-reply", r, m.src, m.view, m.slot, m.value, 0, 0, {}, EmptyIntervals)}
          ELSE messages' = messages \ {m}
    /\ UNCHANGED <<connectionUp, logVars, syncAcks, activeSyncSlot, syncCertificates,
                    viewVars, diskVars, clientVars, scheduleVars>>

\* recovery.go:200-214: replies are accumulated for the active coordinate.
HandleSyncReply(l, m) ==
    /\ m \in messages /\ m.kind = "sync-reply" /\ m.dst = l
    /\ l = Leader(m.view) /\ m.view = view[l]
    /\ activeSyncSlot[l] = m.slot
    /\ syncAcks' = [syncAcks EXCEPT ![l][m.slot] = @ \cup {m.src}]
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, logVars, prepareDisposition, prepareValue,
                    activeSyncSlot, syncCertificates, viewVars, diskVars,
                    clientVars, scheduleVars>>

\* recovery.go:200-220: leader certifies exact local prefix, but wire commit omits hash.
MaybeCommitSync(l, s) ==
    /\ l = Leader(view[l]) /\ Quorum(syncAcks[l][s])
    /\ activeSyncSlot[l] = s /\ s > stableSlot[l]
    /\ prepareDisposition[l][s] = "match" /\ prepareValue[l][s] \in StoredValues
    /\ syncCertificates' = syncCertificates \cup
         {[view |-> view[l], slot |-> s, value |-> prepareValue[l][s]]}
    /\ stableSlot' = [stableSlot EXCEPT ![l] = s]
    \* recovery.go:52-105,200-220: the completed round certifies its captured
    \* prepare hash/value even if memory retention advances while replies arrive.
    /\ stableValue' = [stableValue EXCEPT ![l] =
          [t \in Slots |-> IF t = s THEN prepareValue[l][s]
                           ELSE IF t < s /\ memLog[l][t] # Empty THEN memLog[l][t]
                           ELSE @[t]]]
    /\ activeSyncSlot' = [activeSyncSlot EXCEPT ![l] = 0]
    /\ messages' = messages \cup UNION
         {IF connectionUp[l][r]
             THEN {Msg("sync-commit", l, r, view[l], s, Empty, 0, 0, {}, EmptyIntervals)}
             ELSE {} : r \in Replicas \ {l}}
    /\ UNCHANGED <<connectionUp, memLog, canonicalLog, valueHistory,
                    executedThrough, prepareDisposition, prepareValue, syncAcks,
                    viewVars, diskVars, clientVars, scheduleVars>>

\* recovery.go:222-233: current-view follower accepts coordinate-only commit.
HandleSyncCommit(r, m) ==
    /\ m \in messages /\ m.kind = "sync-commit" /\ m.dst = r
    /\ m.view = view[r] /\ m.src = Leader(view[r])
    /\ m.slot <= executedThrough[r] /\ m.slot > stableSlot[r]
    /\ stableSlot' = [stableSlot EXCEPT ![r] = m.slot]
    /\ stableValue' = [stableValue EXCEPT ![r] =
          [t \in Slots |-> IF t <= m.slot /\ memLog[r][t] # Empty THEN memLog[r][t]
                           ELSE @[t]]]
    /\ Consume(m)
    /\ UNCHANGED <<connectionUp, memLog, canonicalLog, valueHistory,
                    executedThrough, syncVars, viewVars, diskVars,
                    clientVars, scheduleVars>>

\* replica peer transport: links can fail independently and messages can be delayed/lost.
Disconnect(a, b) ==
    /\ a # b /\ connectionUp[a][b]
    /\ connectionUp' = [connectionUp EXCEPT ![a][b] = FALSE]
    /\ UNCHANGED <<messages, logVars, syncVars, viewVars, diskVars,
                    clientVars, scheduleVars>>

Reconnect(a, b) ==
    /\ a # b /\ ~connectionUp[a][b]
    /\ connectionUp' = [connectionUp EXCEPT ![a][b] = TRUE]
    /\ UNCHANGED <<messages, logVars, syncVars, viewVars, diskVars,
                    clientVars, scheduleVars>>

LoseMessage(m) ==
    /\ m \in messages /\ Consume(m)
    /\ UNCHANGED <<connectionUp, logVars, syncVars, viewVars, diskVars,
                    clientVars, scheduleVars>>

\* recovery.go:348-435: suspicion/watchdog may advance one replica independently.
BeginViewChange(r) ==
    /\ view[r] < MaxView
    /\ view' = [view EXCEPT ![r] = @ + 1]
    /\ replicaStatus' = [replicaStatus EXCEPT ![r] = "view-change"]
    /\ reportReceived' = [reportReceived EXCEPT ![Leader(view'[r])] = {}]
    \* schedule.go/recovery.go: prepared suffix remains in history but is uncommitted.
    /\ scheduleEpoch' = [scheduleEpoch EXCEPT ![r] = view'[r]]
    /\ UNCHANGED <<networkVars, logVars, syncVars, reportEvidence,
                    chosenDonor, donorAvailable, fetchAttempts, diskVars,
                    clientVars, scheduleHistory, windowMembers, windowIntervals,
                    preparedWindow, preparedAcks, committedEnd,
                    clientAuthorizedWindow, ownershipSeen>>

ReportBase(r) == IF executedThrough[r] + 1 > MetadataWindow
                 THEN executedThrough[r] - MetadataWindow + 1 ELSE 1
ReportOf(r, s) == IF s < ReportBase(r) THEN "unknown"
                  ELSE IF memLog[r][s] # Empty THEN "present" ELSE "absent"

\* recovery.go:532-703: report bounded suffix evidence and select first donor.
BuildViewChangeReport(r, l) ==
    LET ev == [s \in Slots |-> ReportOf(r, s)] IN
    /\ replicaStatus[r] = "view-change" /\ l = Leader(view[r])
    /\ replicaStatus[l] = "view-change" /\ view[l] = view[r]
    \* recovery.go:449-473,713-729: a replica emits one immutable report per view.
    /\ r \notin reportReceived[l]
    /\ reportReceived' = [reportReceived EXCEPT ![l] = @ \cup {r}]
    /\ reportEvidence' = [reportEvidence EXCEPT ![l][r] = ev]
    \* recovery.go:687-702: only one donor identity survives the merge.
    /\ chosenDonor' = [chosenDonor EXCEPT ![l] =
          [s \in Slots |-> IF @[s] = NoReplica /\ ev[s] = "present" THEN r ELSE @[s]]]
    /\ UNCHANGED <<networkVars, logVars, syncVars, view, replicaStatus,
                    donorAvailable, fetchAttempts, diskVars, clientVars,
                    scheduleVars>>

\* recovery.go:899-927: each attempt retries only the selected donor.
FetchMergedSlot(l, s) ==
    LET d == chosenDonor[l][s]
        x == IF d \in Replicas THEN memLog[d][s] ELSE Empty
    IN
    /\ d \in Replicas /\ fetchAttempts[l][s] < MaxFetchAttempts
    /\ fetchAttempts' = [fetchAttempts EXCEPT ![l][s] = @ + 1]
    /\ IF donorAvailable[d] /\ x # Empty
          THEN /\ memLog' = [memLog EXCEPT ![l][s] = x]
               /\ canonicalLog' = [canonicalLog EXCEPT ![s] = x]
               /\ valueHistory' = [valueHistory EXCEPT ![s] = @ \cup {x}]
               /\ canonicalGeneration' = [canonicalGeneration EXCEPT ![s] =
                                              IF canonicalLog[s] = x THEN @ ELSE @ + 1]
          ELSE UNCHANGED <<memLog, canonicalLog, valueHistory, canonicalGeneration>>
    /\ UNCHANGED <<networkVars, executedThrough, stableSlot, stableValue,
                    syncVars, view, replicaStatus, reportReceived, reportEvidence,
                    chosenDonor, donorAvailable, diskLog, diskGeneration, pruned,
                    transferValue, transferFromDisk, transferGeneration, clientVars, scheduleVars>>

\* recovery.go:930-945,674-703: exhausted/absent/unknown evidence closes a no-op.
CloseMergedSlotAsNoOp(l, s) ==
    /\ replicaStatus[l] = "view-change" /\ Quorum(reportReceived[l])
    /\ memLog[l][s] = Empty
    \* mergeSuffix only closes holes through the highest filled slot in the
    \* selected reports; an entirely empty suffix has no canonical boundary.
    /\ \E q \in reportReceived[l], t \in Slots :
          /\ t >= s /\ reportEvidence[l][q][t] = "present"
    /\ \/ /\ chosenDonor[l][s] \in Replicas
           /\ fetchAttempts[l][s] = MaxFetchAttempts
       \/ /\ chosenDonor[l][s] = NoReplica
           /\ \A r \in reportReceived[l] : reportEvidence[l][r][s] # "present"
    /\ memLog' = [memLog EXCEPT ![l][s] = NoOp]
    /\ canonicalLog' = [canonicalLog EXCEPT ![s] = NoOp]
    /\ valueHistory' = [valueHistory EXCEPT ![s] = @ \cup {NoOp}]
    /\ canonicalGeneration' = [canonicalGeneration EXCEPT ![s] = @ + 1]
    /\ UNCHANGED <<networkVars, executedThrough, stableSlot, stableValue,
                    syncVars, viewVars, diskLog, diskGeneration, pruned,
                    transferValue, transferFromDisk, transferGeneration, clientVars, scheduleVars>>

\* recovery.go:1204-1288,1428-1501: rewind and install canonical replacement.
InstallCanonicalSlot(r, s, x) ==
    /\ replicaStatus[r] = "view-change" /\ x \in StoredValues /\ memLog[r][s] # x
    /\ canonicalLog[s] = x
    /\ memLog' = [memLog EXCEPT ![r][s] = x]
    \* recovery.go:1258-1265,1458-1463: rewind execution before replacement.
    /\ executedThrough' = [executedThrough EXCEPT ![r] =
                              IF s <= @ THEN s - 1 ELSE @]
    /\ UNCHANGED <<networkVars, canonicalLog, valueHistory, stableSlot,
                    stableValue, syncVars, viewVars, diskLog, diskGeneration,
                    canonicalGeneration, pruned, transferValue,
                    transferFromDisk, transferGeneration, clientVars, scheduleVars>>

ReportBoundsSlot(l, s) ==
    \E q \in reportReceived[l], t \in Slots :
      /\ t >= s /\ reportEvidence[l][q][t] = "present"

RecoveryPlanInstalled(l) ==
    /\ Quorum(reportReceived[l])
    /\ \A s \in Slots : ReportBoundsSlot(l, s) => canonicalLog[s] # Empty

\* recovery.go:1394-1413,1489-1512: publish normal after canonical install.
FinishViewChange(r) ==
    /\ replicaStatus[r] = "view-change"
    \* driveViewChange/installStartView publish normal only after a leader
    \* report quorum and materialization of the selected canonical range.
    /\ RecoveryPlanInstalled(Leader(view[r]))
    /\ \A s \in Slots : canonicalLog[s] # Empty => memLog[r][s] = canonicalLog[s]
    /\ replicaStatus' = [replicaStatus EXCEPT ![r] = "normal"]
    \* recovery.go:1398-1406: commit installed rolling history in the new epoch.
    /\ scheduleEpoch' = [scheduleEpoch EXCEPT ![r] = view[r]]
    /\ committedEnd' = [committedEnd EXCEPT ![r] =
                          IF scheduleHistory[r] = {} THEN 0 ELSE MaxWindowOf(scheduleHistory[r])]
    /\ UNCHANGED <<networkVars, logVars, syncVars, view, reportReceived,
                    reportEvidence, chosenDonor, donorAvailable, fetchAttempts,
                    diskVars, clientVars, scheduleHistory, windowMembers,
                    windowIntervals, preparedWindow, preparedAcks,
                    clientAuthorizedWindow, ownershipSeen>>

\* recovery.go:899-945: crash-stop donor loss after its report.
SetDonorAvailable(r, b) ==
    /\ donorAvailable[r] # b
    /\ donorAvailable' = [donorAvailable EXCEPT ![r] = b]
    /\ UNCHANGED <<networkVars, logVars, syncVars, view, replicaStatus,
                    reportReceived, reportEvidence, chosenDonor, fetchAttempts,
                    diskVars, clientVars, scheduleVars>>

\* replica.go:1218-1251: before first sync, limit remains nextExpected.
PruneCommittedSlot(r, s) ==
    /\ s \notin pruned[r] /\ s < executedThrough[r]
    /\ s + RetainWindow < executedThrough[r]
    /\ (stableSlot[r] = 0 \/ s <= stableSlot[r])
    /\ memLog' = [memLog EXCEPT ![r][s] = Empty]
    /\ pruned' = [pruned EXCEPT ![r] = @ \cup {s}]
    /\ UNCHANGED <<networkVars, canonicalLog, valueHistory, executedThrough,
                    stableSlot, stableValue, syncVars, viewVars, diskLog,
                    diskGeneration, canonicalGeneration, transferValue,
                    transferFromDisk, transferGeneration, clientVars, scheduleVars>>

\* recovery.go:2057-2084 and logreader.go:110-188: memory then settled disk.
ServeState(from, to, s) ==
    /\ from # to /\ s \in Slots
    /\ memLog[to][s] = Empty
    /\ IF memLog[from][s] # Empty
          THEN /\ transferValue' = [transferValue EXCEPT ![to][s] = memLog[from][s]]
               /\ transferFromDisk' = [transferFromDisk EXCEPT ![to][s] = FALSE]
               /\ transferGeneration' = [transferGeneration EXCEPT ![to][s] = 0]
               /\ memLog' = [memLog EXCEPT ![to][s] = memLog[from][s]]
          ELSE /\ s \in pruned[from] /\ s <= stableSlot[from]
               /\ diskLog[from][s] # Empty
               /\ transferValue' = [transferValue EXCEPT ![to][s] = diskLog[from][s]]
               /\ transferFromDisk' = [transferFromDisk EXCEPT ![to][s] = TRUE]
               /\ transferGeneration' = [transferGeneration EXCEPT ![to][s] = diskGeneration[from][s]]
               /\ memLog' = [memLog EXCEPT ![to][s] = diskLog[from][s]]
    \* recovery.go:1909-1950: a successfully received state reply is installed
    \* under the receiver lock before the fetch is reported complete.
    /\ UNCHANGED <<networkVars, canonicalLog, valueHistory, executedThrough,
                    stableSlot, stableValue, syncVars, viewVars, diskLog,
                    diskGeneration, canonicalGeneration, pruned,
                    clientVars, scheduleVars>>

\* client_control.go:131-181: control sockets reconnect independently.
SetControlConnection(c, r, up) ==
    /\ controlConnUp[c][r] # up
    /\ controlConnUp' = [controlConnUp EXCEPT ![c][r] = up]
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskVars,
                    deliveredReplies, clientCommitted, controlReports,
                    clientPauseView, clientActiveView, clientPaused,
                    dataConnUp, scheduleVars>>

\* client_control.go:184-208: record report; certification needs quorum+leader.
HandleControlReport(c, r) ==
    /\ controlConnUp[c][r]
    /\ controlReports' = [controlReports EXCEPT ![c][r] =
                           [view |-> view[r], normal |-> replicaStatus[r] = "normal"]]
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskVars,
                    deliveredReplies, clientCommitted, clientPauseView,
                    clientActiveView, clientPaused, controlConnUp, dataConnUp,
                    scheduleVars>>

CertifiedView(c, v) ==
    LET normal == {r \in Replicas : controlReports[c][r].view = v /\
                                     controlReports[c][r].normal} IN
    /\ Quorum(normal) /\ Leader(v) \in normal

\* client_control.go:209-224: every received status independently invokes pauseForView.
PauseForReportedView(c, r) ==
    LET v == controlReports[c][r].view IN
    /\ v > clientActiveView[c] /\ v > clientPauseView[c]
    /\ clientPaused' = [clientPaused EXCEPT ![c] = TRUE]
    /\ clientPauseView' = [clientPauseView EXCEPT ![c] = v]
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskVars,
                    deliveredReplies, clientCommitted, controlReports,
                    clientActiveView, controlConnUp, dataConnUp, scheduleVars>>

\* client schedule/control completion: resume only with leader-inclusive normal quorum.
ResumeCertifiedView(c, v) ==
    /\ clientPaused[c] /\ v = clientPauseView[c] /\ CertifiedView(c, v)
    /\ clientPaused' = [clientPaused EXCEPT ![c] = FALSE]
    /\ clientActiveView' = [clientActiveView EXCEPT ![c] = v]
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskVars,
                    deliveredReplies, clientCommitted, controlReports,
                    clientPauseView, controlConnUp, dataConnUp, scheduleVars>>

\* client.go:84-115,236-273,451-470: a data sender/receiver dies permanently.
FailDataConnection(c, r) ==
    /\ dataConnUp[c][r]
    /\ dataConnUp' = [dataConnUp EXCEPT ![c][r] = FALSE]
    /\ UNCHANGED <<networkVars, logVars, syncVars, viewVars, diskVars,
                    deliveredReplies, clientCommitted, controlReports,
                    clientPauseView, clientActiveView, clientPaused,
                    controlConnUp, scheduleVars>>

\* ----------------------------- Invariants -----------------------------
TypeOK ==
    /\ messages \subseteq Message
    /\ connectionUp \in [Replicas -> [Replicas -> BOOLEAN]]
    /\ memLog \in [Replicas -> [Slots -> LogValues]]
    /\ canonicalLog \in [Slots -> LogValues]
    /\ valueHistory \in [Slots -> SUBSET StoredValues]
    /\ executedThrough \in [Replicas -> 0..MaxSlot]
    /\ stableSlot \in [Replicas -> 0..MaxSlot]
    /\ stableValue \in [Replicas -> [Slots -> LogValues]]
    /\ prepareDisposition \in [Replicas -> [Slots -> Disposition]]
    /\ prepareValue \in [Replicas -> [Slots -> LogValues]]
    /\ syncAcks \in [Replicas -> [Slots -> SUBSET Replicas]]
    /\ activeSyncSlot \in [Replicas -> 0..MaxSlot]
    /\ syncCertificates \subseteq [view : Views, slot : Slots, value : StoredValues]
    /\ view \in [Replicas -> Views] /\ replicaStatus \in [Replicas -> Status]
    /\ reportReceived \in [Replicas -> SUBSET Replicas]
    /\ reportEvidence \in [Replicas -> [Replicas -> [Slots -> Evidence]]]
    /\ chosenDonor \in [Replicas -> [Slots -> Replicas \cup {NoReplica}]]
    /\ donorAvailable \in [Replicas -> BOOLEAN]
    /\ fetchAttempts \in [Replicas -> [Slots -> 0..MaxFetchAttempts]]
    /\ diskLog \in [Replicas -> [Slots -> LogValues]]
    /\ diskGeneration \in [Replicas -> [Slots -> Nat]]
    /\ canonicalGeneration \in [Slots -> Nat]
    /\ pruned \in [Replicas -> SUBSET Slots]
    /\ transferValue \in [Replicas -> [Slots -> LogValues]]
    /\ transferFromDisk \in [Replicas -> [Slots -> BOOLEAN]]
    /\ transferGeneration \in [Replicas -> [Slots -> Nat]]
    /\ deliveredReplies \in [RealValues -> SUBSET Replicas]
    /\ clientCommitted \subseteq RealValues
    /\ scheduleHistory \in [Replicas -> SUBSET Windows]
    /\ windowMembers \in [Replicas -> [Windows -> SUBSET Clients]]
    /\ windowIntervals \in [Replicas -> [Windows -> [Clients -> 0..1]]]
    /\ preparedWindow \in [Replicas -> 0..MaxWindow]
    /\ preparedAcks \in [Replicas -> SUBSET Replicas]
    /\ committedEnd \in [Replicas -> 0..MaxWindow]
    /\ scheduleEpoch \in [Replicas -> Views]
    /\ clientAuthorizedWindow \in [Clients -> 0..MaxWindow]
    /\ ownershipSeen \in [Slots -> SUBSET Clients]
    /\ controlReports \in [Clients -> [Replicas -> [view : Views, normal : BOOLEAN]]]
    /\ clientPauseView \in [Clients -> Views]
    /\ clientActiveView \in [Clients -> Views]
    /\ clientPaused \in [Clients -> BOOLEAN]
    /\ controlConnUp \in [Clients -> [Replicas -> BOOLEAN]]
    /\ dataConnUp \in [Clients -> [Replicas -> BOOLEAN]]

ExecutedPrefixBounded == \A r \in Replicas : stableSlot[r] <= executedThrough[r]
SlotOwnershipUnique == \A s \in Slots : Cardinality(ownershipSeen[s]) <= 1
ScheduleHistoryCompatible ==
    \A r, q \in Replicas :
      (replicaStatus[r] = "normal" /\ replicaStatus[q] = "normal" /\ view[r] = view[q]) =>
        \A w \in 1..Min2(committedEnd[r], committedEnd[q]) :
          /\ w \in scheduleHistory[r] /\ w \in scheduleHistory[q]
          /\ windowMembers[r][w] = windowMembers[q][w]
          /\ windowIntervals[r][w] = windowIntervals[q][w]
SlotAgreement == \A s \in Slots : Cardinality(valueHistory[s] \cap RealValues) <= 1
StablePrefixesAgree ==
    \A r, q \in Replicas : \A s \in 1..Min2(stableSlot[r], stableSlot[q]) :
      stableValue[r][s] = stableValue[q][s]
StableHasMatchingCertificate ==
    \A r \in Replicas : stableSlot[r] > 0 =>
      [view |-> view[r], slot |-> stableSlot[r],
       value |-> stableValue[r][stableSlot[r]]] \in syncCertificates
CommittedRequestSurvivesViewChange ==
    \A x \in clientCommitted : \E s \in Slots : canonicalLog[s] = x
NoCommittedBusBecomesNoOp ==
    \A s \in Slots : NoOp \in valueHistory[s] =>
      (valueHistory[s] \cap clientCommitted) = {}
DurableStateMatchesCanonical ==
    \A r \in Replicas, s \in Slots :
      transferFromDisk[r][s] /\ transferValue[r][s] # Empty =>
        /\ transferValue[r][s] = canonicalLog[s]
        /\ transferGeneration[r][s] = canonicalGeneration[s]
Authoritative(r, s) ==
    memLog[r][s] = canonicalLog[s] \/
      (s \in pruned[r] /\ diskLog[r][s] = canonicalLog[s] /\
       diskGeneration[r][s] = canonicalGeneration[s])
CanonicalDataHasRecoverableSource ==
    \A s \in Slots :
      (canonicalLog[s] # Empty /\ \E r \in Replicas : s <= stableSlot[r]) =>
        \E r \in Replicas : donorAvailable[r] /\ Authoritative(r, s)
PauseRequiresCertifiedView ==
    \A c \in Clients : clientPaused[c] /\ clientPauseView[c] > clientActiveView[c] =>
      CertifiedView(c, clientPauseView[c])
PruningRespectsStableFrontier ==
    \A r \in Replicas : \A s \in pruned[r] : stableSlot[r] > 0 => s <= stableSlot[r]

Next ==
    \/ \E c \in Clients, l \in Replicas, w \in Windows, i \in 0..1 : RequestScheduleWindow(c,l,w,i)
    \/ \E l \in Replicas, w \in Windows : LeaderPrepareSchedule(l,w)
    \/ \E r \in Replicas, m \in messages : HandleSchedulePrepare(r,m)
    \/ \E l \in Replicas, m \in messages : HandleScheduleReply(l,m)
    \/ \E l \in Replicas : LeaderCommitSchedule(l)
    \/ \E r \in Replicas, m \in messages : HandleScheduleCommit(r,m)
    \/ \E c \in Clients, x \in RealValues, r \in Replicas, w \in Windows : ClientSendsBus(c,x,r,w)
    \/ \E r \in Replicas, m \in messages : HandleBus(r,m)
    \/ \E r \in Replicas, s \in Slots : ExecuteSlot(r,s)
    \/ \E c \in Clients, m \in messages : ClientHandlesReply(c,m)
    \/ \E c \in Clients, x \in RealValues : ClientCommits(c,x)
    \/ \E l,r \in Replicas, s \in Slots : SendSyncPrepare(l,r,s)
    \/ \E r \in Replicas, m \in messages : HandleSyncPrepare(r,m)
    \/ \E l \in Replicas, m \in messages : HandleSyncReply(l,m)
    \/ \E l \in Replicas, s \in Slots : MaybeCommitSync(l,s)
    \/ \E r \in Replicas, m \in messages : HandleSyncCommit(r,m)
    \/ \E a,b \in Replicas : Disconnect(a,b) \/ Reconnect(a,b)
    \/ \E m \in messages : LoseMessage(m)
    \/ \E r \in Replicas : BeginViewChange(r)
    \/ \E r,l \in Replicas : BuildViewChangeReport(r,l)
    \/ \E l \in Replicas, s \in Slots : FetchMergedSlot(l,s) \/ CloseMergedSlotAsNoOp(l,s)
    \/ \E r \in Replicas, s \in Slots, x \in StoredValues : InstallCanonicalSlot(r,s,x)
    \/ \E r \in Replicas : FinishViewChange(r)
    \/ \E r \in Replicas, b \in BOOLEAN : SetDonorAvailable(r,b)
    \/ \E r \in Replicas, s \in Slots : PruneCommittedSlot(r,s)
    \/ \E a,b \in Replicas, s \in Slots : ServeState(a,b,s)
    \/ \E c \in Clients, r \in Replicas, b \in BOOLEAN : SetControlConnection(c,r,b)
    \/ \E c \in Clients, r \in Replicas : HandleControlReport(c,r) \/ PauseForReportedView(c,r)
    \/ \E c \in Clients, v \in Views : ResumeCertifiedView(c,v)
    \/ \E c \in Clients, r \in Replicas : FailDataConnection(c,r)

ReplicaOrderValue == <<r0, r1, r2>>
ClientOrderValue == <<c0, c1>>
Spec == Init /\ [][Next]_vars
=============================================================================
