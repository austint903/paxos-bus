------------------------------ MODULE paxosbus --------------------------------
EXTENDS Naturals, Sequences, FiniteSets

\* Deterministic ordering for both replica and client 
CONSTANTS Replicas, ReplicaOrder

CONSTANTS Clients, ClientOrder

\* Possible values that the slots can hold
CONSTANTS Values, NoOp, Empty

CONSTANTS BusesPerClient, \* Max num of buses per client
          MaxReqNum,      \* Max num of requests per client
          MaxViewID       \* Max num of view changes

\* Replica status
CONSTANTS StNormal, StViewChange

\* Message Types
CONSTANTS MBus,          
          MRequestReply,  
          MGapRequest,    
          MGapReply,     
          MGapCommit,     
          MGapCommitRep,  
          MSyncPrepare,   
          MSyncRep,      
          MSyncCommit,    
          MGetState,
          MNewState,
          MViewChangeReq, 
          MViewChange,    
          MStartView     

ASSUME /\ IsFiniteSet(Replicas)
       /\ ReplicaOrder \in Seq(Replicas)
       /\ Len(ReplicaOrder) = Cardinality(Replicas)
       /\ Cardinality(Replicas) % 2 = 1
ASSUME /\ IsFiniteSet(Clients)
       /\ ClientOrder \in Seq(Clients)
       /\ Len(ClientOrder) = Cardinality(Clients)
ASSUME /\ BusesPerClient \in Nat \ {0}
       /\ MaxReqNum \in Nat \ {0}
       /\ MaxViewID \in Nat
ASSUME /\ NoOp \notin Values
       /\ Empty \notin Values
       /\ NoOp # Empty

NumSlots == Cardinality(Clients) * BusesPerClient
Slots    == 1..NumSlots
BusNums  == 1..BusesPerClient
ReqNums  == 1..MaxReqNum
ViewIDs  == 0..MaxViewID


\* Deterministic ordering of bus slots  
LogIndices == 1..(NumSlots * MaxReqNum)
ClientRank(c) == CHOOSE i \in 1..Len(ClientOrder) : ClientOrder[i] = c
BusSlot(c, n) == (n - 1) * Len(ClientOrder) + ClientRank(c)

Requests   == [ clientID : Clients, requestID : ReqNums, op : Values ]
Buses      == UNION { [ 1..k -> Requests ] : k \in 0..MaxReqNum } \* possible bus values
SlotValues == Buses \cup {NoOp} \*Bus slot values include noop
LogValues  == SlotValues \cup {Empty} 

IsBus(x)   == x # NoOp /\ x # Empty

Logs == [ Slots -> LogValues ]

(*
  `^\textbf{Message Schemas}^'

  Bus
      [ mtype    |-> MBus,
        dest     |-> r \in Replicas,
        sender   |-> c \in Clients,
        busNum   |-> n \in BusNums,
        slot     |-> s \in Slots,          \* = BusSlot(sender, busNum)
        bus      |-> b \in Buses ]         \* the whole passenger list

  RequestReply
      [ mtype    |-> MRequestReply,
        sender   |-> r \in Replicas,
        dest     |-> c \in Clients,
        request  |-> q \in Requests,
        viewID   |-> v \in ViewIDs,
        busSlot  |-> s \in Slots,
        logIndex |-> i \in LogIndices ]    \* first occurrence, per dedup

  GapRequest                               \* both directions, as in the Go
      [ mtype    |-> MGapRequest,
        dest     |-> r \in Replicas,
        sender   |-> r \in Replicas,
        viewID   |-> v \in ViewIDs,
        slot     |-> s \in Slots ]

  GapReply
      [ mtype    |-> MGapReply,
        dest     |-> r \in Replicas,
        sender   |-> r \in Replicas,
        viewID   |-> v \in ViewIDs,
        slot     |-> s \in Slots,
        found    |-> f \in BOOLEAN,
        bus      |-> b \in LogValues ]     \* the passenger list, when found

  GapCommit / GapCommitRep
      [ mtype    |-> MGapCommit or MGapCommitRep,
        dest     |-> r \in Replicas,
        sender   |-> r \in Replicas,
        viewID   |-> v \in ViewIDs,
        slot     |-> s \in Slots ]

  SyncPrepare
      [ mtype    |-> MSyncPrepare,
        dest     |-> r \in Replicas,
        sender   |-> r \in Replicas,
        viewID   |-> v \in ViewIDs,
        slot     |-> s \in Slots,          \* last slot the leader executed
        log      |-> l \in Logs ]          \* the leader's prefix through slot

  SyncRep / SyncCommit
      [ mtype    |-> MSyncRep or MSyncCommit,
        dest     |-> r \in Replicas,
        sender   |-> r \in Replicas,
        viewID   |-> v \in ViewIDs,
        slot     |-> s \in Slots ]

  GetState                                 \* replica -> leader, asks for [from, to]
      [ mtype    |-> MGetState,
        dest     |-> r \in Replicas,
        sender   |-> r \in Replicas,
        viewID   |-> v \in ViewIDs,
        from     |-> s \in 1..(NumSlots+1),
        to       |-> s \in 0..NumSlots ]

  NewState                                 \* leader -> replica, answers a GetState
      [ mtype    |-> MNewState,
        dest     |-> r \in Replicas,
        sender   |-> r \in Replicas,
        viewID   |-> v \in ViewIDs,
        from     |-> s \in 1..(NumSlots+1),
        to       |-> s \in 0..NumSlots,
        log      |-> l \in Logs ]

  ViewChangeReq
      [ mtype    |-> MViewChangeReq,
        dest     |-> r \in Replicas,
        sender   |-> r \in Replicas,
        viewID   |-> v \in ViewIDs ]

  ViewChange                               \* one replica's report
      [ mtype      |-> MViewChange,
        dest       |-> r \in Replicas,     \* = Leader(viewID)
        sender     |-> r \in Replicas,
        viewID     |-> v \in ViewIDs,
        lastNormal |-> v \in ViewIDs,
        syncPoint  |-> s \in 0..NumSlots,
        nextSlot   |-> s \in 1..(NumSlots+1),
        log        |-> l \in Logs ]

  StartView
      [ mtype     |-> MStartView,
        dest      |-> r \in Replicas,
        sender    |-> r \in Replicas,      \* = Leader(viewID)
        viewID    |-> v \in ViewIDs,
        syncPoint |-> s \in 0..NumSlots,   \* merged commit point
        maxSlot   |-> s \in 0..NumSlots,   \* merged suffix end
        log       |-> l \in Logs ]         \* the merged log

*)

--------------------------------------------------------------------------------
(* `^\textbf{\large Variables}^' *)

\* `^\textbf{Network State}^'
VARIABLE messages \* All msgs

networkVars      == << messages >>
InitNetworkState == messages = {}

\* `^\textbf{Replica State}^'
VARIABLES vLog,            \* collection of all rep logs
          vNextExpected,   \* simulates cursor
          vReplicaStatus,  
          vViewID,         
          vLastNormView,  
          vSyncPoint,      \* Commit point
          vTentativeSync,  \* Slot of init sync msg
          vSyncReps,       \* Sync replies
          vViewChangeReqs, \* View change req
          vReportSent,     \* Only one viewChange msg containing log is sent
          vViewChanges,    \* how many view change msgs for new leader
          vGapSlots        \* Slots in gap agreement

coreVars    == << vLog, vNextExpected, vReplicaStatus, vViewID, vLastNormView >>
syncVars    == << vSyncPoint, vTentativeSync, vSyncReps >>
vcVars      == << vViewChangeReqs, vReportSent, vViewChanges >>
gapVars     == << vGapSlots >>
replicaVars == << coreVars, syncVars, vcVars, gapVars >>

InitReplicaState ==
  /\ vLog            = [ r \in Replicas |-> [ s \in Slots |-> Empty ] ]
  /\ vNextExpected   = [ r \in Replicas |-> 1 ]
  /\ vReplicaStatus  = [ r \in Replicas |-> StNormal ]
  /\ vViewID         = [ r \in Replicas |-> 0 ]
  /\ vLastNormView   = [ r \in Replicas |-> 0 ]
  /\ vSyncPoint      = [ r \in Replicas |-> 0 ]
  /\ vTentativeSync  = [ r \in Replicas |-> 0 ]
  /\ vSyncReps       = [ r \in Replicas |-> {} ]
  /\ vViewChangeReqs = [ r \in Replicas |-> {} ]
  /\ vReportSent     = [ r \in Replicas |-> FALSE ]
  /\ vViewChanges    = [ r \in Replicas |-> {} ]
  /\ vGapSlots       = [ r \in Replicas |-> {} ]

\* `^\textbf{Client State}^'
VARIABLES vPending,    \* Set of all unboarded request, empty after each bus
          vBusPayload  \* what each bus carried, to ensure payloads are same for each bus

clientVars      == << vPending, vBusPayload >>
InitClientState ==
  /\ vPending    = [ c \in Clients |-> {} ]
  /\ vBusPayload = [ c \in Clients |-> [ n \in BusNums |-> Empty ] ]

vars == << networkVars, replicaVars, clientVars >>

\* `^\textbf{Initial state}^'
Init == /\ InitNetworkState
        /\ InitReplicaState
        /\ InitClientState

(* `^\textbf{Helpers}^' *)

Max(S) == CHOOSE x \in S : \A y \in S : x >= y
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

Quorums    == {R \in SUBSET(Replicas) : Cardinality(R) * 2 > Cardinality(Replicas)}
QuorumSize == (Cardinality(Replicas) \div 2) + 1

\* Get leader for viewID
Leader(viewID) == ReplicaOrder[(viewID % Len(ReplicaOrder)) + 1]

\* Send message
Send(ms) == messages' = messages \cup ms

\* `^\textbf{Log Helpers}^'

\* equivalent of max slot seen as in the code, largest slot
FilledSlots(log) == {s \in Slots : log[s] # Empty}
MaxFilled(log)   == IF FilledSlots(log) = {} THEN 0 ELSE Max(FilledSlots(log))

\* Copy of log up to k (commit point), simulates hash
Prefix(log, k) == [ s \in Slots |-> IF s <= k THEN log[s] ELSE Empty ]

\* Helper that flattens bus requests for a replica
RECURSIVE ReqSeq(_, _)
ReqSeq(log, k) ==
  IF k = 0 THEN
    << >>
  ELSE
    LET p == ReqSeq(log, k - 1)
    IN IF IsBus(log[k]) THEN p \o log[k] ELSE p

ReqLog(r) == ReqSeq(vLog[r], vNextExpected[r] - 1)


\* This checks for dedup, same implementation idea as in the go code
LogIndexOf(seq, q) ==
  CHOOSE i \in 1..Len(seq) :
    /\ seq[i] = q
    /\ \A j \in 1..Len(seq) : seq[j] = q => i <= j

\* Sync helper - any replica that is missing slots up to latest commit point can fetch from src (leader)
FillMissing(log, src, k) ==
  [ s \in Slots |-> IF s <= k /\ log[s] = Empty THEN src[s] ELSE log[s] ]

\* `^\textbf{Reply Helpers}^'

\* Set of replies for every request carried by a bus, accounts for duplicate via logindexof
RepliesForSlot(r, s, seq, log, v) ==
  { [ mtype    |-> MRequestReply,
      sender   |-> r,
      dest     |-> log[s][i].clientID,
      request  |-> log[s][i],
      viewID   |-> v,
      busSlot  |-> s,
      logIndex |-> LogIndexOf(seq, log[s][i]) ] : i \in 1..Len(log[s]) }

\* Used after view change, sends replies for every slot in the replica's log after view change. This includes 
\* the merged suffix after view change. In the code, we only send replies for the merged suffix. We simplify here 
\* by sending a reply for every slot in the log because performance is not an issue
RepliesFor(r, log, k, v) ==
  LET seq == ReqSeq(log, k)
  IN UNION { RepliesForSlot(r, s, seq, log, v) :
             s \in {t \in 1..k : IsBus(log[t])} }

--------------------------------------------------------------------------------
(* `^\textbf{Message Handlers}^' *)

(* `^\textbf{Client actions}^' *)
\* Prevent reusing same request ID and help with retrying req
SeqToSet(sq)  == { sq[i] : i \in 1..Len(sq) } 
Departed(c)   == { n \in BusNums : vBusPayload[c][n] # Empty } \* Num of buses
Boarded(c)    == UNION { SeqToSet(vBusPayload[c][n]) : n \in Departed(c) }
Created(c)    == vPending[c] \cup Boarded(c)
NextReqID(c)  == Cardinality({ q.requestID : q \in Created(c) }) + 1

\* Requests have deterministic order in buses
RECURSIVE OrderByID(_, _)
OrderByID(S, k) ==
  IF k = 0 THEN
    << >>
  ELSE
    LET p == OrderByID(S, k - 1)
    IN IF \E q \in S : q.requestID = k
       THEN Append(p, CHOOSE q \in S : q.requestID = k)
       ELSE p
BoardingOrder(S) == OrderByID(S, MaxReqNum)

\* Client request generation loop
ClientGenerates(c, op) ==
  /\ NextReqID(c) \in ReqNums
  /\ vPending' = [ vPending EXCEPT ![c] = @ \cup
                     {[ clientID  |-> c,
                        requestID |-> NextReqID(c),
                        op        |-> op ]} ]
  /\ UNCHANGED << vBusPayload, networkVars, replicaVars >>

\* Request retires
ClientReboards(c, q) ==
  /\ q \in Boarded(c)
  /\ q \notin vPending[c]
  /\ vPending' = [ vPending EXCEPT ![c] = @ \cup {q} ]
  /\ UNCHANGED << vBusPayload, networkVars, replicaVars >>


\* Client c sends bus n
ClientSendsBus(c, n) ==
  LET bus == BoardingOrder(vPending[c])
  IN /\ vBusPayload[c][n] = Empty
     /\ \A k \in 1..(n - 1) : vBusPayload[c][k] # Empty \* Every lower number bus departed 
     /\ vBusPayload' = [ vBusPayload EXCEPT ![c][n] = bus ]
     /\ vPending'    = [ vPending EXCEPT ![c] = {} ]
     /\ Send({[ mtype  |-> MBus,
                dest   |-> d,
                sender |-> c,
                busNum |-> n,
                slot   |-> BusSlot(c, n),
                bus    |-> bus ] : d \in Replicas})
     /\ UNCHANGED replicaVars

(* `^\textbf{Normal Case Handlers}^' *)

\* Replica side handling bus
HandleBus(r, m) ==
  /\ vLog[r][m.slot] = Empty
  /\ vLog' = [ vLog EXCEPT ![r][m.slot] = m.bus ]
  /\ UNCHANGED << vNextExpected, vReplicaStatus, vViewID, vLastNormView,
                  syncVars, vcVars, gapVars, networkVars, clientVars >>

\* Executing the bus in slot, decomposes requests into flat list and sends replies for all of them
ExecuteSlot(r) ==
  LET s == vNextExpected[r]
  IN /\ vReplicaStatus[r] = StNormal
     /\ s \in Slots
     /\ vLog[r][s] # Empty
     /\ vNextExpected' = [ vNextExpected EXCEPT ![r] = s + 1 ]
     /\ IF IsBus(vLog[r][s])
        THEN Send(RepliesForSlot(r, s, ReqSeq(vLog[r], s), vLog[r], vViewID[r]))
        ELSE UNCHANGED networkVars
     /\ UNCHANGED << vLog, vReplicaStatus, vViewID, vLastNormView,
                     syncVars, vcVars, gapVars, clientVars >>

(* `^\textbf{Gap Recovery Handlers}^' *)

\* Replica detecting a gap and then asking leader for it
AskLeaderForGap(r, s) ==
  /\ vReplicaStatus[r] = StNormal
  /\ r # Leader(vViewID[r])
  /\ vLog[r][s] = Empty
  /\ \E t \in (s + 1)..NumSlots : vLog[r][t] # Empty
  /\ Send({[ mtype  |-> MGapRequest,
             dest   |-> Leader(vViewID[r]),
             sender |-> r,
             viewID |-> vViewID[r],
             slot   |-> s ]})
  /\ UNCHANGED << replicaVars, clientVars >>

\* The leader finds a gap of its own and asks every follower for it.
LeaderStartsGapProbe(r, s) ==
  /\ vReplicaStatus[r] = StNormal
  /\ Leader(vViewID[r]) = r
  /\ vLog[r][s] = Empty
  /\ \E t \in (s + 1)..NumSlots : vLog[r][t] # Empty
  /\ vGapSlots' = [ vGapSlots EXCEPT ![r] = @ \cup {s} ]
  /\ Send({[ mtype  |-> MGapRequest,
             dest   |-> d,
             sender |-> r,
             viewID |-> vViewID[r],
             slot   |-> s ] : d \in Replicas \ {r}})
  /\ UNCHANGED << coreVars, syncVars, vcVars, clientVars >>

\* Handles gap request message, chooses one of 4 messages
HandleGapRequest(r, m) ==
  /\ vReplicaStatus[r] = StNormal
  /\ m.viewID = vViewID[r]
  /\ LET leader == Leader(vViewID[r])
     IN \/ \* follower answers leader
           /\ r # leader
           /\ m.sender = leader
           /\ Send({[ mtype   |-> MGapReply,
                      dest    |-> leader,
                      sender  |-> r,
                      viewID  |-> m.viewID,
                      slot    |-> m.slot,
                      found   |-> IsBus(vLog[r][m.slot]),
                      bus     |-> vLog[r][m.slot] ]})
           /\ UNCHANGED << replicaVars, clientVars >>
        \/ \* Leader handles the gap
           /\ r = leader
           /\ IsBus(vLog[r][m.slot])
           /\ Send({[ mtype   |-> MGapReply,
                      dest    |-> m.sender,
                      sender  |-> r,
                      viewID  |-> m.viewID,
                      slot    |-> m.slot,
                      found   |-> TRUE,
                      bus     |-> vLog[r][m.slot] ]})
           /\ UNCHANGED << replicaVars, clientVars >>
        \/ \* Leader handles whens lot is noop
           /\ r = leader
           /\ vLog[r][m.slot] = NoOp
           /\ Send({[ mtype  |-> MGapCommit,
                      dest   |-> m.sender,
                      sender |-> r,
                      viewID |-> m.viewID,
                      slot   |-> m.slot ]})
           /\ UNCHANGED << replicaVars, clientVars >>
        \/ \* Leader is also missing message
           /\ r = leader
           /\ vLog[r][m.slot] = Empty
           /\ vGapSlots' = [ vGapSlots EXCEPT ![r] = @ \cup {m.slot} ]
           /\ Send({[ mtype  |-> MGapRequest,
                      dest   |-> d,
                      sender |-> r,
                      viewID |-> m.viewID,
                      slot   |-> m.slot ] : d \in Replicas \ {r}})
           /\ UNCHANGED << coreVars, syncVars, vcVars, clientVars >>

\* Either non leader replica or leader recieves a reply, only proceeds if it responds with valid bus
HandleGapReply(r, m) ==
  /\ vReplicaStatus[r] = StNormal
  /\ m.viewID = vViewID[r]
  /\ m.found \* Only proceed if it carries bus
  /\ vLog[r][m.slot] = Empty
  /\ \/ r = Leader(vViewID[r])         \* Leader gets reply from follower after probing for missing slot
     \/ m.sender = Leader(vViewID[r])  \* Leader responds 
  /\ vLog' = [ vLog EXCEPT ![r][m.slot] = m.bus ]
  /\ UNCHANGED << vNextExpected, vReplicaStatus, vViewID, vLastNormView,
                  syncVars, vcVars, gapVars, networkVars, clientVars >>

\* Leader replica commits no-op
LeaderCommitsGap(r, s) ==
  /\ vReplicaStatus[r] = StNormal
  /\ Leader(vViewID[r]) = r
  /\ s \in vGapSlots[r] \* If it is in registred gaps for the leader
  /\ vLog[r][s] = Empty
  /\ vLog' = [ vLog EXCEPT ![r][s] = NoOp ]
  /\ Send({[ mtype  |-> MGapCommit,
             dest   |-> d,
             sender |-> r,
             viewID |-> vViewID[r],
             slot   |-> s ] : d \in Replicas \ {r}})
  /\ UNCHANGED << vNextExpected, vReplicaStatus, vViewID, vLastNormView,
                  syncVars, vcVars, gapVars, clientVars >>

\* When rep gets gap commit, if it already has no-op, reply. If slot either bus or empty
\* still commit no-op and reply
HandleGapCommit(r, m) ==
  /\ vReplicaStatus[r] = StNormal
  /\ m.viewID = vViewID[r]
  /\ m.sender = Leader(vViewID[r])
  /\ \/ vLog[r][m.slot] = NoOp
     \/ m.slot >= vNextExpected[r]
  /\ vLog' = [ vLog EXCEPT ![r][m.slot] = NoOp ]
  /\ Send({[ mtype  |-> MGapCommitRep,
             dest   |-> m.sender,
             sender |-> r,
             viewID |-> m.viewID,
             slot   |-> m.slot ]})
  /\ UNCHANGED << vNextExpected, vReplicaStatus, vViewID, vLastNormView,
                  syncVars, vcVars, gapVars, clientVars >>

(* `^\textbf{Synchronization Handlers / Heartbeats}^' *)

\* Leader starts a heartbeat round with the most up to date slot (last slot)
StartSync(r) ==
  LET k == vNextExpected[r] - 1
  IN /\ vReplicaStatus[r] = StNormal
     /\ Leader(vViewID[r]) = r
     /\ k \in Slots
     /\ vTentativeSync' = [ vTentativeSync EXCEPT ![r] = k ]
     /\ vSyncReps'      = [ vSyncReps EXCEPT ![r] = {} ]
     /\ Send({[ mtype  |-> MSyncPrepare,
                dest   |-> d,
                sender |-> r,
                viewID |-> vViewID[r],
                slot   |-> k,
                log    |-> Prefix(vLog[r], k) ] : d \in Replicas \ {r}})
     /\ UNCHANGED << coreVars, vSyncPoint, vcVars, gapVars, clientVars >>

\* Non leader replica receieves the sync message from leader and its log is 
\* at least up to the slot the leader synced with (k). It then sends reply
ReplyToSyncPrepare(r, m) ==
  /\ vReplicaStatus[r] = StNormal
  /\ m.viewID = vViewID[r]
  /\ m.sender = Leader(vViewID[r])
  /\ vNextExpected[r] - 1 >= m.slot
  /\ Prefix(vLog[r], m.slot) = m.log
  /\ Send({[ mtype  |-> MSyncRep,
             dest   |-> m.sender,
             sender |-> r,
             viewID |-> m.viewID,
             slot   |-> m.slot ]})
  /\ UNCHANGED << replicaVars, clientVars >>

\* After getting heartbeat, it is behind so it asks the leader for the missing range.
\* The prefix arrives with mnewstate from leader
RequestSyncPrefix(r, m) ==
  /\ vReplicaStatus[r] = StNormal
  /\ m.viewID = vViewID[r]
  /\ m.sender = Leader(vViewID[r])
  /\ vNextExpected[r] - 1 < m.slot
  /\ Send({[ mtype  |-> MGetState,
             dest   |-> m.sender,
             sender |-> r,
             viewID |-> m.viewID,
             from   |-> vNextExpected[r],
             to     |-> m.slot ]})
  /\ UNCHANGED << replicaVars, clientVars >>

\* Repairs replica r's log when mismatch with the leader's log, rewinds to K (commit point)
\* and rebuild the log.
RepairSyncMismatch(r, m) ==
  LET k == vSyncPoint[r]
  IN /\ vReplicaStatus[r] = StNormal
     /\ m.viewID = vViewID[r]
     /\ m.sender = Leader(vViewID[r])
     /\ vNextExpected[r] - 1 >= m.slot
     /\ Prefix(vLog[r], m.slot) # m.log
     /\ k > 0                             \* no commit point: nothing to rewind to
     /\ MaxFilled(vLog[r]) > k            \* nothing above it
     /\ vLog' = [ vLog EXCEPT ![r] =
                    [ s \in Slots |-> IF s <= k THEN vLog[r][s] ELSE Empty ] ]
     /\ vNextExpected' = [ vNextExpected EXCEPT ![r] = k + 1 ]
     /\ Send({[ mtype  |-> MGetState,
                dest   |-> m.sender,
                sender |-> r,
                viewID |-> m.viewID,
                from   |-> k + 1,
                to     |-> MaxFilled(vLog[r]) ]})
     /\ UNCHANGED << vReplicaStatus, vViewID, vLastNormView,
                     syncVars, vcVars, gapVars, clientVars >>

\* Split msync prepare to 3diff handlers with 3 diff message types
HandleSyncPrepare(r, m) ==
  \/ ReplyToSyncPrepare(r, m)
  \/ RequestSyncPrefix(r, m)
  \/ RepairSyncMismatch(r, m)

\* Leader handles the sending back state to replica requesting
HandleGetState(r, m) ==
  /\ m.viewID = vViewID[r]
  /\ Send({[ mtype   |-> MNewState,
             dest    |-> m.sender,
             sender  |-> r,
             viewID  |-> m.viewID,
             from    |-> m.from,
             to      |-> m.to,
             log     |-> [ s \in Slots |->
                           IF s >= m.from /\ s <= m.to THEN vLog[r][s] ELSE Empty ] ]})
  /\ UNCHANGED << replicaVars, clientVars >>

\* Once leader sends new state, handle it
HandleNewState(r, m) ==
  /\ m.viewID = vViewID[r]
  /\ m.sender = Leader(vViewID[r])
  /\ \E s \in Slots : /\ s >= m.from
                      /\ s <= m.to
                      /\ vLog[r][s] = Empty
                      /\ m.log[s] # Empty
  /\ vLog' = [ vLog EXCEPT ![r] =
                 [ s \in Slots |-> IF s >= m.from /\ s <= m.to /\ vLog[r][s] = Empty
                                   THEN m.log[s]
                                   ELSE vLog[r][s] ] ]
  /\ UNCHANGED << vNextExpected, vReplicaStatus, vViewID, vLastNormView,
                  syncVars, vcVars, gapVars, networkVars, clientVars >>

\* After leader gets quroum for heartbeats, it can send sync commit and update its own commit point
HandleSyncRep(r, m) ==
  /\ vReplicaStatus[r] = StNormal
  /\ Leader(vViewID[r]) = r
  /\ m.viewID = vViewID[r]
  /\ m.slot = vTentativeSync[r]
  /\ vSyncReps' = [ vSyncReps EXCEPT ![r] = @ \cup {m} ]
  /\ LET voters == { n.sender : n \in vSyncReps'[r] } \cup {r}
     IN IF voters \in Quorums
        THEN /\ vSyncPoint' = [ vSyncPoint EXCEPT ![r] = m.slot ]
             /\ Send({[ mtype  |-> MSyncCommit,
                        dest   |-> d,
                        sender |-> r,
                        viewID |-> vViewID[r],
                        slot   |-> m.slot ] : d \in Replicas \ {r}})
        ELSE UNCHANGED << vSyncPoint, networkVars >>
  /\ UNCHANGED << coreVars, vTentativeSync, vcVars, gapVars, clientVars >>

\* Replica gets syncCommit; updates their local commit point, should be <= their latest executed slot
HandleSyncCommit(r, m) ==
  /\ vReplicaStatus[r] = StNormal
  /\ m.viewID = vViewID[r]
  /\ m.sender = Leader(vViewID[r])
  /\ m.slot < vNextExpected[r]
  /\ m.slot > vSyncPoint[r]
  /\ vSyncPoint' = [ vSyncPoint EXCEPT ![r] = m.slot ]
  /\ UNCHANGED << coreVars, vTentativeSync, vSyncReps, vcVars, gapVars,
                  networkVars, clientVars >>

(* `^\textbf{View Change Handlers}^' *)

\* View change request
ViewChangeReqs(r, v) ==
  {[ mtype  |-> MViewChangeReq,
     dest   |-> d,
     sender |-> r,
     viewID |-> v ] : d \in Replicas \ {r}}

\* View change msg to leader after quroum of view change requests
ViewChangeReport(r, v) ==
  [ mtype      |-> MViewChange,
    dest       |-> Leader(v),
    sender     |-> r,
    viewID     |-> v,
    lastNormal |-> vLastNormView[r],
    syncPoint  |-> vSyncPoint[r],
    nextSlot   |-> vNextExpected[r],
    log        |-> vLog[r] ]

\* Starts when replica determines timeout. It sends the ViewChangeReqs
StartViewChange(r) ==
  LET v == vViewID[r] + 1
  IN /\ v <= MaxViewID
     /\ vViewID'         = [ vViewID EXCEPT ![r] = v ]
     /\ vReplicaStatus'  = [ vReplicaStatus EXCEPT ![r] = StViewChange ]
     /\ vViewChangeReqs' = [ vViewChangeReqs EXCEPT ![r] = {r} ]
     /\ vReportSent'     = [ vReportSent EXCEPT ![r] = FALSE ]
     /\ vViewChanges'    = [ vViewChanges EXCEPT ![r] = {} ]
     /\ vGapSlots'       = [ vGapSlots EXCEPT ![r] = {} ]
     /\ vTentativeSync'  = [ vTentativeSync EXCEPT ![r] = 0 ]
     /\ vSyncReps'       = [ vSyncReps EXCEPT ![r] = {} ]
     /\ Send(ViewChangeReqs(r, v))
     /\ UNCHANGED << vLog, vNextExpected, vLastNormView, vSyncPoint, clientVars >>

\* Once a replica joins new view change and gets quorum of ViewChangeRequsts, it can send report the new leader
JoinViewChange(r, m) ==
  LET v       == m.viewID
      reqs    == {r, m.sender}
      sending == reqs \in Quorums
  IN /\ v > vViewID[r]
     /\ v <= MaxViewID
     /\ vViewID'         = [ vViewID EXCEPT ![r] = v ]
     /\ vReplicaStatus'  = [ vReplicaStatus EXCEPT ![r] = StViewChange ]
     /\ vViewChangeReqs' = [ vViewChangeReqs EXCEPT ![r] = reqs ]
     /\ vReportSent'     = [ vReportSent EXCEPT ![r] = sending ]
     /\ vViewChanges'    = [ vViewChanges EXCEPT ![r] = {} ]
     /\ vGapSlots'       = [ vGapSlots EXCEPT ![r] = {} ]
     /\ vTentativeSync'  = [ vTentativeSync EXCEPT ![r] = 0 ]
     /\ vSyncReps'       = [ vSyncReps EXCEPT ![r] = {} ]
     /\ Send(ViewChangeReqs(r, v) \cup
             (IF sending THEN {ViewChangeReport(r, v)} ELSE {}))
     /\ UNCHANGED << vLog, vNextExpected, vLastNormView, vSyncPoint, clientVars >>

\* Records the view change request sent multicasted by all replicas when detecting leader failure
\* If we already sent the report, then it doens't run again, else it records the reply contributing to quorum
RecordViewChangeReq(r, m) ==
  LET reqs    == vViewChangeReqs[r] \cup {m.sender}
      sending == reqs \in Quorums
  IN /\ m.viewID = vViewID[r]
     /\ vReplicaStatus[r] = StViewChange
     /\ ~vReportSent[r]
     /\ m.sender \notin vViewChangeReqs[r]
     /\ vViewChangeReqs' = [ vViewChangeReqs EXCEPT ![r] = reqs ]
     /\ vReportSent'     = [ vReportSent EXCEPT ![r] = sending ]
     /\ IF sending
        THEN Send({ViewChangeReport(r, m.viewID)})
        ELSE UNCHANGED networkVars
     /\ UNCHANGED << coreVars, syncVars, vViewChanges, gapVars, clientVars >>

HandleViewChangeReq(r, m) ==
  \/ JoinViewChange(r, m)
  \/ RecordViewChangeReq(r, m)

(*
  `^\textbf{View change merge}^'
*)
\* Helpers
BestLastNormal(reports)  == Max({ n.lastNormal : n \in reports })
Survivors(reports)       == { n \in reports : n.lastNormal = BestLastNormal(reports) } \* Logs in max(lastNormalView)
MergedSyncPoint(reports) == Max({ n.syncPoint : n \in Survivors(reports) })
MergedMaxSlot(reports)   == Max({ MaxFilled(n.log) : n \in Survivors(reports) } \cup {MergedSyncPoint(reports)})

\* Merge the entire log. If below commit point, just take value (or no-op if all doesn't have a value)
\* Above commit point, we do standard merge logic. 
MergedLog(reports) ==
  LET stable    == MergedSyncPoint(reports)
      top       == MergedMaxSlot(reports)
      donors(s) == { n.log[s] : n \in Survivors(reports) } \ {Empty}
  IN [ s \in Slots |->
         IF s > top THEN
           Empty
         ELSE IF s > stable /\ NoOp \in donors(s) THEN
           NoOp
         ELSE IF donors(s) \ {NoOp} # {} THEN
           CHOOSE q \in donors(s) \ {NoOp} : TRUE
         ELSE
           NoOp ]

\* Message sent to all replicas after log merge is done; sends the updated merged
StartViews(r, v, reports) ==
  {[ mtype     |-> MStartView,
     dest      |-> d,
     sender    |-> r,
     viewID    |-> v,
     syncPoint |-> MergedSyncPoint(reports),
     maxSlot   |-> MergedMaxSlot(reports),
     log       |-> MergedLog(reports) ] : d \in Replicas}

      
\* New Leader runs this when they have quroum of reports (replica logs). Sends start view message 
\* which calculates the merged log and then sends to replicas.
HandleViewChange(r, m) ==
  LET before == { n.sender : n \in {x \in vViewChanges[r] : x.viewID = vViewID[r]} }
  IN /\ vViewID[r] = m.viewID
     /\ vReplicaStatus[r] = StViewChange
     /\ Leader(vViewID[r]) = r
     /\ m \notin vViewChanges[r]
     /\ vViewChanges' = [ vViewChanges EXCEPT ![r] = @ \cup {m} ]
     /\ LET reports == { n \in vViewChanges'[r] : n.viewID = vViewID[r] }
        IN IF /\ { n.sender : n \in reports } \in Quorums
              /\ ~(before \in Quorums)
           THEN Send(StartViews(r, vViewID[r], reports))
           ELSE UNCHANGED networkVars
     /\ UNCHANGED << coreVars, syncVars, vViewChangeReqs, vReportSent,
                     gapVars, clientVars >>


\* When replica receiving a StartView from the leader, they update their log. We send the entire log, unlike in the code
\* so it is just one step to install. Replies are then sent for every request in the new merged log
HandleStartView(r, m) ==
  /\ m.sender = Leader(m.viewID)
  /\ \/ m.viewID > vViewID[r]
     \/ /\ m.viewID = vViewID[r]
        /\ vReplicaStatus[r] = StViewChange
  /\ m.maxSlot >= vSyncPoint[r]
  /\ vLog'            = [ vLog EXCEPT ![r] = m.log ]
  /\ vNextExpected'   = [ vNextExpected EXCEPT ![r] = m.maxSlot + 1 ]
  /\ vReplicaStatus'  = [ vReplicaStatus EXCEPT ![r] = StNormal ]
  /\ vViewID'         = [ vViewID EXCEPT ![r] = m.viewID ]
  /\ vLastNormView'   = [ vLastNormView EXCEPT ![r] = m.viewID ]
  /\ vSyncPoint'      = [ vSyncPoint EXCEPT ![r] = Max({@, m.syncPoint}) ]
  /\ vTentativeSync'  = [ vTentativeSync EXCEPT ![r] = 0 ]
  /\ vSyncReps'       = [ vSyncReps EXCEPT ![r] = {} ]
  /\ vViewChangeReqs' = [ vViewChangeReqs EXCEPT ![r] = {} ]
  /\ vReportSent'     = [ vReportSent EXCEPT ![r] = FALSE ]
  /\ vViewChanges'    = [ vViewChanges EXCEPT ![r] = {} ]
  /\ vGapSlots'       = [ vGapSlots EXCEPT ![r] = {} ]
  /\ Send(RepliesFor(r, m.log, m.maxSlot, m.viewID))
  /\ UNCHANGED clientVars

--------------------------------------------------------------------------------
(* `^\textbf{\large Invariants and Helper Functions}^' *)

\* checks if a request was commited at slot i in view v
CommittedInView(q, i, v) ==
  LET senders == { m.sender : m \in {x \in messages :
                     /\ x.mtype    = MRequestReply
                     /\ x.request  = q
                     /\ x.logIndex = i
                     /\ x.viewID   = v} }
  IN /\ senders \in Quorums
     /\ Leader(v) \in senders

\* See if a request was commited in slot i in any view
Committed(q, i) == \E v \in ViewIDs : CommittedInView(q, i, v)

\* If no op was commited for a slot
NoOpAcks(v, s) ==
  { m.sender : m \in {x \in messages : /\ x.mtype  = MGapCommitRep
                                       /\ x.viewID = v
                                       /\ x.slot   = s
                                       /\ x.dest   = Leader(v)} }

NoOpChosen(v, s) ==
  /\ \E m \in messages : /\ m.mtype  = MGapCommit
                         /\ m.viewID = v
                         /\ m.slot   = s
                         /\ m.sender = Leader(v)
  /\ (NoOpAcks(v, s) \cup {Leader(v)}) \in Quorums

\* The same commit rule for bus
SlotCommitted(q, s, v) ==
  \E i \in LogIndices :
    LET senders == { m.sender : m \in {x \in messages :
                       /\ x.mtype    = MRequestReply
                       /\ x.request  = q
                       /\ x.busSlot  = s
                       /\ x.logIndex = i
                       /\ x.viewID   = v} }
    IN /\ senders \in Quorums
       /\ Leader(v) \in senders

\* Ensures that the durability invariant is meaningful such that v has had a quroum progress through v
SystemRecovered(v) ==
  /\ \E R \in Quorums :
       \A r \in R : /\ vReplicaStatus[r] = StNormal
                    /\ vLastNormView[r] >= v
  /\ vLastNormView[Leader(v)] >= v

(* `^\textbf{Invariants}^' *)

\* Type correctness invariant
TypeOK ==
  /\ \A m \in messages :
       m.mtype \in {MBus, MRequestReply, MGapRequest, MGapReply,
                    MGapCommit, MGapCommitRep, MSyncPrepare, MSyncRep,
                    MSyncCommit, MGetState, MNewState, MViewChangeReq,
                    MViewChange, MStartView}
  /\ vLog            \in [ Replicas -> Logs ]
  /\ vNextExpected   \in [ Replicas -> 1..(NumSlots + 1) ]
  /\ vReplicaStatus  \in [ Replicas -> {StNormal, StViewChange} ]
  /\ vViewID         \in [ Replicas -> ViewIDs ]
  /\ vLastNormView   \in [ Replicas -> ViewIDs ]
  /\ vSyncPoint      \in [ Replicas -> 0..NumSlots ]
  /\ vTentativeSync  \in [ Replicas -> 0..NumSlots ]
  /\ vSyncReps       \in [ Replicas -> SUBSET messages ]
  /\ \A r \in Replicas : \A m \in vSyncReps[r] : m.mtype = MSyncRep
  /\ vViewChangeReqs \in [ Replicas -> SUBSET Replicas ]
  /\ vReportSent     \in [ Replicas -> BOOLEAN ]
  /\ vViewChanges    \in [ Replicas -> SUBSET messages ]
  /\ \A r \in Replicas : \A m \in vViewChanges[r] : m.mtype = MViewChange
  /\ vGapSlots       \in [ Replicas -> SUBSET Slots ]
  /\ vPending        \in [ Clients -> SUBSET Requests ]
  /\ vBusPayload     \in [ Clients -> [ BusNums -> Buses \cup {Empty} ] ]
  /\ \A r \in Replicas : vLastNormView[r] <= vViewID[r]

DeterministicSchedule ==
  /\ { BusSlot(c, n) : c \in Clients, n \in BusNums } = Slots
  /\ \A c1, c2 \in Clients, n1, n2 \in BusNums :
       BusSlot(c1, n1) = BusSlot(c2, n2) => (c1 = c2 /\ n1 = n2)

\* The cursor only runs over filled buses (empty or not)
ExecutedPrefixFilled ==
  \A r \in Replicas : \A s \in 1..(vNextExpected[r] - 1) : vLog[r][s] # Empty

\* The commit point never claims more than this replica has executed
SyncPointExecuted ==
  \A r \in Replicas : vSyncPoint[r] < vNextExpected[r]

\* A slot only ever holds the bus the schedule assigned to it (deterministic scheduling)
ScheduleRespected ==
  \A r \in Replicas, c \in Clients, n \in BusNums :
    IsBus(vLog[r][BusSlot(c, n)]) =>
      vLog[r][BusSlot(c, n)] = vBusPayload[c][n]

\* Replica slots should be identical when we deterministically assign their schedules
SlotAgreement ==
  \A r1, r2 \in Replicas, s \in Slots :
    (IsBus(vLog[r1][s]) /\ IsBus(vLog[r2][s])) =>
      vLog[r1][s] = vLog[r2][s]

\* Only one request can be committed at a log index
Linearizability ==
  \A q1, q2 \in Requests, i \in LogIndices :
    (Committed(q1, i) /\ Committed(q2, i)) => q1 = q2

\* If a request was commited at some previous view, in later view, that request is durable
Durability ==
  \A q \in Requests, i \in LogIndices, v1, v2 \in ViewIDs :
    ( /\ v1 < v2
      /\ SystemRecovered(v2)
      /\ CommittedInView(q, i, v1) )
    => \E v3 \in v2..MaxViewID : CommittedInView(q, i, v3)

\* Everything at or below a replica's commit point is committed.
SyncSafety ==
  \A r \in Replicas :
    \A s \in 1..vSyncPoint[r] :
      IsBus(vLog[r][s]) =>
        \A i \in 1..Len(vLog[r][s]) :
          Committed(vLog[r][s][i], LogIndexOf(ReqLog(r), vLog[r][s][i]))

\* A no-op remains in the log of every replica that installs in a later view.
NoOpLogDurability ==
  \A v \in ViewIDs, s \in Slots, r \in Replicas :
    (NoOpChosen(v, s) /\ vLastNormView[r] > v)
      => vLog[r][s] = NoOp

\* Ensure that up to an agreed commit point, they have the same log prefix
StablePrefixesAgree ==
  \A r1, r2 \in Replicas :
    \A s \in 1..Min({vSyncPoint[r1], vSyncPoint[r2]}) :
      vLog[r1][s] = vLog[r2][s]

--------------------------------------------------------------------------------
(* `^\textbf{\large State transitions}^' *)

Next == \* Handle Messages
        \/ \E m \in messages : /\ m.mtype = MBus
                               /\ HandleBus(m.dest, m)
        \/ \E m \in messages : /\ m.mtype = MGapRequest
                               /\ HandleGapRequest(m.dest, m)
        \/ \E m \in messages : /\ m.mtype = MGapReply
                               /\ HandleGapReply(m.dest, m)
        \/ \E m \in messages : /\ m.mtype = MGapCommit
                               /\ HandleGapCommit(m.dest, m)
        \/ \E m \in messages : /\ m.mtype = MSyncPrepare
                               /\ HandleSyncPrepare(m.dest, m)
        \/ \E m \in messages : /\ m.mtype = MGetState
                               /\ HandleGetState(m.dest, m)
        \/ \E m \in messages : /\ m.mtype = MNewState
                               /\ HandleNewState(m.dest, m)
        \/ \E m \in messages : /\ m.mtype = MSyncRep
                               /\ HandleSyncRep(m.dest, m)
        \/ \E m \in messages : /\ m.mtype = MSyncCommit
                               /\ HandleSyncCommit(m.dest, m)
        \/ \E m \in messages : /\ m.mtype = MViewChangeReq
                               /\ HandleViewChangeReq(m.dest, m)
        \/ \E m \in messages : /\ m.mtype = MViewChange
                               /\ HandleViewChange(m.dest, m)
        \/ \E m \in messages : /\ m.mtype = MStartView
                               /\ HandleStartView(m.dest, m)
        \* Client Actions
        \/ \E c \in Clients, op \in Values : ClientGenerates(c, op)
        \/ \E c \in Clients, q \in Boarded(c) : ClientReboards(c, q)
        \/ \E c \in Clients, n \in BusNums : ClientSendsBus(c, n)
        \* Ordered execution
        \/ \E r \in Replicas : ExecuteSlot(r)
        \* Gap detection
        \/ \E r \in Replicas, s \in Slots : AskLeaderForGap(r, s)
        \/ \E r \in Replicas, s \in Slots : LeaderStartsGapProbe(r, s)
        \/ \E r \in Replicas, s \in Slots : LeaderCommitsGap(r, s)
        \* Start synchronization
        \/ \E r \in Replicas : StartSync(r)
        \* Failure case: real leader failure, or a false suspicion
        \/ \E r \in Replicas : StartViewChange(r)

Spec == Init /\ [][Next]_vars

================================================================================
