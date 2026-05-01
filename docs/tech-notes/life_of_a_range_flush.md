# Life of a Range Flush

A range flush moves data from the **store-local engine** (the per-node Pebble
instance) to the **range-shared engine** (a per-range virtual LSM backed by
shared storage). This document walks through the complete lifecycle of a single
range flush, detailing the concurrency control mechanisms and the hazards they
guard against.

## Background: Two Engines

Every range's replicated state lives in two places:

```
┌───────────────────────────────────────────────────┐
│                Store-Local Engine                 │
│  (single Pebble instance, shared by all ranges    │
│   on this store; serves reads immediately)        │
└────────────────────────┬──────────────────────────┘
                         │  range flush
                         ▼
┌───────────────────────────────────────────────────┐
│               Range-Shared Engine                 │
│  (per-range virtual LSM backed by shared storage; │
│   each range has its own manifest and SSTs)       │
└───────────────────────────────────────────────────┘
```

Writes always land in the store-local engine via Raft. A range flush
periodically snapshots the range's data from store-local, writes it to an SST,
installs it into the range-shared engine, and then logically deletes the
store-local copy. Reads merge results from both engines.

## Overview: Two-Phase Protocol

A range flush is a two-phase protocol — **prepare** and **commit** — separated
by an off-Raft SST creation step. Only the leaseholder initiates flushes. The
entire protocol runs outside any transaction.

```
  Leaseholder (RangeFlush)
  ════════════════════════

  ┌───────────────────────┐
  │ 1. PREPARE            │
  │    • Raft proposal    │──────► Raft log
  │    • Dormant clears   │        (all replicas apply)
  │    • Increment count  │
  │    • Take snapshot    │◄────── proposer-only side effect
  └───────────┬───────────┘
              │
  ┌───────────▼───────────┐
  │ 2. CREATE SST         │  (off-Raft, leaseholder only)
  │    • Iterate snapshot │
  │    • Write SST file   │
  │    • Hardlink to all  │
  │      replica dirs     │
  └───────────┬───────────┘
              │
  ┌───────────▼───────────┐
  │ 3. COMMIT             │
  │    • Raft proposal    │──────► Raft log
  │    • Activate clears  │        (all replicas apply)
  │    • Install manifest │
  │    • Swap RSEngine    │◄────── each replica's side effect
  └───────────────────────┘
```

## Phase 1: Prepare

### 1.1 Entry: RangeFlush

The flush begins in `RangeFlush` on the leaseholder. Before doing anything with
Raft, it performs two local checks:

1. **Lease check**: Verifies this replica is the leaseholder.
2. **rangeFlushMu**: Acquires the mutex and checks `ongoingFlush`. If another
   flush is already in progress, returns immediately. Otherwise, sets
   `ongoingFlush = true` and increments a local `flushCount` (used for unique
   scratch filenames).

```
  rangeFlushMu
  ┌──────────────────────────────┐
  │ ongoingFlush: bool           │ ◄── at most one flush at a time
  │ flushCount:   uint64         │ ◄── unique scratch filename
  │ snapshot:     Reader         │ ◄── set during application
  │ approxStoreLocalBytes: int64 │ ◄── captured with snapshot
  └──────────────────────────────┘
```

### 1.2 Sending the Prepare Request

`RangeFlush` constructs a `RangeFlushPrepareRequest` spanning the range and
sends it through Raft via `kv.SendWrappedWith`. The request carries:

- The range span `[StartKey, EndKey)` from the current descriptor.
- `DormantSpans`: key spans over which to write `ClearRawRangeDormant`.

### 1.3 Prepare: Request Flags and Latching

The prepare request has the following flags:

```
  RangeFlushPrepareRequest.flags():
      isWrite | isRange | isAlone | isUnsplittable
```

**`isAlone`** is required because the snapshot is taken in `prepareLocalResult`
after the evaluation batch applies. If other requests followed the prepare in
the same batch, the snapshot would include their writes — data that the dormant
clears don't cover. This would break the flush boundary: the SST would contain
writes that survive activation in the store-local engine, causing them to appear
in both engines.

**Latching**: The prepare latches on `RangeDescriptorKey`, the same key that
the flush commit (`SetRangeSharedManifestNum`) and splits/merges latch on:

```
  declareKeysRangeFlushPrepare():
      latch on RangeDescriptorKey(startKey)  [read-write]
```

This is critical for the correctness of the `FlushStartedCount` protocol and
the dormant/activate protocol. Without this latch, a second prepare could be
proposed to Raft after a commit evaluates (passing the count check) but before
it applies, leading to premature activation of the second dormant and data
loss. See **Hazard 3** for the detailed scenario.

Despite the latch, concurrent reads and writes to user data proceed without
interference — the `RangeDescriptorKey` latch only conflicts with other
operations that also latch on it (splits, merges, other prepares, and commits).
The dormant clears themselves are invisible to readers until activated, and
when activated the data will have moved to the range shared engine, so readers
will again be unaware.

### 1.4 Prepare: Evaluation

During evaluation on each replica:

1. **Increment `FlushStartedCount`** in `RangeAppliedState`. This is a
   monotonically increasing counter in replicated state. The new value is
   returned in the response.
2. **Write `ClearRawRangeDormant`** over each span in `DormantSpans`. These
   dormant markers are inert until activated. They are written to the Pebble
   batch and will be applied atomically with the state change.

### 1.5 Prepare: Application Side Effect (Proposer Only)

After the Raft command commits and applies, the proposer (the leaseholder that
initiated the flush) executes a local side effect in `prepareLocalResult`:

```
  prepareLocalResult (under raftMu):
    rangeFlushMu.Lock()
    if rangeFlushMu.ongoingFlush:
        rangeFlushMu.snapshot = storeLocalEngine.NewSnapshot()
        rangeFlushMu.approxStoreLocalBytes = <current value>
    rangeFlushMu.Unlock()
```

The snapshot is taken **after** the prepare batch has been applied to the
store-local engine, so it includes the dormant clears and all writes up to that
point. The snapshot is a Pebble point-in-time snapshot — concurrent writes after
this point are not visible through it.

### 1.6 Why the Snapshot is Taken During Application

The snapshot must be taken at application time (under `raftMu`) rather than at
send time because:

- The prepare might have been proposed but not yet applied. Other writes could
  have applied between the proposal and application.
- The snapshot must reflect exactly the state after the dormant clears are
  written, ensuring the SST captures everything that the dormant clears will
  eventually shadow.

```
  Time ──────────────────────────────────────────────────►

  Proposer sends prepare
       │
       ▼
  ┌─ Other Raft commands apply ──┐
  │   (writes to keys a, b, c)   │
  └──────────────────────────────┘
       │
       ▼
  Prepare command applies
  ┌──────────────────────────┐
  │ 1. Increment FlushCount  │
  │ 2. Write dormant clears  │
  │ 3. Batch commits         │
  │ 4. Take snapshot ◄───────│── includes a, b, c AND dormant clears
  └──────────────────────────┘
```

## Phase 2: Create SST (Off-Raft)

After `kv.SendWrappedWith` returns, `RangeFlush` picks up the snapshot from
`rangeFlushMu` and iterates it to create an SST file.

### 2.1 Snapshot Iteration

The snapshot is iterated over the range's replicated key spans (user keys). Each
point key and range deletion is written to a new SST file on the basalt
filesystem. The SST is written to a scratch directory with a unique filename
derived from `flushCount`.

### 2.2 Hardlinking to Replica Directories

After the SST is finalized, `InstallNewManifest` creates hardlinks for the SST
and manifest files in every other replica's directory. This ensures all replicas
have access to the new files before the Raft command that switches manifests.

### 2.3 Concurrent Activity During SST Creation

While the SST is being created, **all normal Raft operations continue**:

- Writes land in the store-local engine and are served from there.
- Reads continue to work (the dormant clears are invisible).
- Splits and merges can occur (validated later).

This is the beauty of the two-phase design: the prepare imposes minimal
disruption, and the potentially slow SST creation happens entirely off the
critical path.

## Phase 3: Commit

### 3.1 Pre-Commit Validation

Before sending the commit request, `RangeFlush` validates that the range span
hasn't changed since prepare:

```
  currentDesc := r.Desc()
  if prepareDesc.StartKey != currentDesc.StartKey ||
     prepareDesc.EndKey != currentDesc.EndKey:
      → error: "range span changed since flush prepare"
```

If the range split or merged (and then re-merged back to the same span), the
SST will still be valid.

### 3.2 Sending the Commit Request

The commit goes through the RSEngine's `FlushSSTables`, which calls
`InstallNewManifest`. These two methods contribute different fields to the
Raft request, guarding different windows:

**RSEngine (`FlushSSTables`)**: Takes a snapshot of its own state to build the
new manifest. The snapshot's manifest number becomes `ExpectedManifestNum` —
the predecessor manifest that the new manifest is derived from. The new
manifest says "take everything in manifest N, add these files." This field
guards the window from **RSEngine snapshot → Raft evaluation**: if a
concurrent compaction changed the manifest from N to N' in that window,
installing this manifest would revert to N's file set, losing the
compaction's changes.

**`InstallNewManifest`**: Reads the **current** descriptor and creates
hardlinks for the SST and manifest files in every other replica's directory
based on the current replica set. The descriptor's generation becomes
`ExpectedDescGeneration`. This field guards the narrower window from
**hardlink creation → Raft evaluation**: if a split/merge changed the
replicas in that window, the hardlinks would be for the wrong set of
replicas.

| Field | Set by | Window guarded |
|-------|--------|----------------|
| `ExpectedManifestNum` | RSEngine snapshot | RSEngine snapshot → Commit Raft evaluation |
| `ExpectedDescGeneration` | `InstallNewManifest` | Hardlink creation → Commit Raft evaluation |
| `NextManifestNum` | RSEngine | The new manifest to install |
| `IsFlushCommit = true` | `RangeFlush` | Distinguishes from compaction installs |
| `ExpectedFlushStartedCount` | Prepare response | Prepare → Commit Raft evaluation |
| `ActivateSpans` | `RangeFlush` | Key spans for `ClearRawRangeActivate` |

Between prepare and commit, splits/merges and compactions can freely occur.
Only `ExpectedFlushStartedCount` spans from prepare to commit.
`ExpectedManifestNum` spans from the RSEngine snapshot (within
`FlushSSTables`) to Raft evaluation. `ExpectedDescGeneration` spans from
`InstallNewManifest` to Raft evaluation.

### 3.3 Commit: Latching

The commit request has flags and latching matching the prepare:

```
  SetRangeSharedManifestNumRequest.flags():
      isWrite | isRange | isAlone | isUnsplittable

  declareKeysSetRangeSharedManifestNum():
      latch on RangeDescriptorKey(startKey)  [read-write]
```

- **`isRange`**: The request spans the entire range.
- **`isAlone`**: The request must be alone in a batch — no other requests can
  piggyback.
- **`isUnsplittable`**: The DistSender must not split this request across range
  boundaries.
- **`RangeDescriptorKey` latch**: Mutual exclusion with splits, merges, and
  flush prepares. The shared latch between prepare and commit serializes their
  evaluations and thus their Raft proposal order, which is critical for the
  `FlushStartedCount` and dormant/activate protocols (see **Hazard 3**).

### 3.4 Commit: Evaluation

During evaluation on each replica, the command performs checks guarding two
different windows:

**Guards within the commit phase**:

1. **Verify `ExpectedManifestNum`**: Read `RSManifestState` and check the
   manifest number matches. A mismatch means a concurrent compaction or
   another flush changed the manifest after the RSEngine took its snapshot
   (in `FlushSSTables`) but before this Raft command evaluated. The new
   manifest was built as a successor to the expected manifest — it says
   "take manifest N's files, add these new files." Installing it on a
   different base would lose whatever the concurrent compaction added.

2. **Verify `ExpectedDescGeneration`**: Read the `RangeDescriptor` from
   storage using an MVCC read at the request's timestamp. A mismatch means
   a split/merge changed the descriptor after `InstallNewManifest` created
   hardlinks for a specific set of replicas. The MVCC read (rather than
   using the in-memory descriptor) is critical: if a concurrent split/merge
   transaction has written a provisional (intent) descriptor, this read
   returns a `WriteIntentError`, catching splits/merges that haven't
   committed yet.

**Guard for the prepare-to-commit window**:

3. **Verify `ExpectedFlushStartedCount`**: Check that `FlushStartedCount`
   hasn't changed since prepare. If another prepare has intervened, the
   count will be higher and this commit must fail — it was built from a
   stale snapshot. The `RangeDescriptorKey` latch is scoped to a single
   evaluation and does not span from prepare to commit.
   `FlushStartedCount` is the only mechanism that guards this window.
   It is replicated state, so it also detects prepares initiated by a
   different leaseholder after a lease transfer (see **Hazard 3**).

**Effect**:

4. **Write `ClearRawRangeActivate`** over each span in `ActivateSpans`. This
   activates the dormant clears written during prepare. From this point,
   the store-local data under those spans is logically deleted.

5. **Set up `RSManifestInstall`** in the replicated result. The actual
   RSManifestState write and RSEngine swap happen during application
   (post-trigger), not during evaluation.

### 3.5 Commit: Application (All Replicas)

When the commit command applies on each replica:

1. The evaluation batch (containing `ClearRawRangeActivate` writes) is committed
   to the store-local Pebble.
2. The `RSManifestInstall` trigger fires, which:
   - Writes the new `RSManifestState` to the store-local engine.
   - Swaps the `rsEngine` pointer to a new RSEngine opened at the new manifest.
3. These happen **under `rsStateMu` (write lock)**, ensuring atomicity between
   the state change and the engine swap.

```
  rsStateMu (write lock):
  ┌──────────────────────────────────────────────┐
  │ 1. Commit batch with ClearRawRangeActivate   │
  │    (store-local data now logically gone)     │
  │                                              │
  │ 2. Write new RSManifestState                 │
  │                                              │
  │ 3. Close old RSEngine, open new RSEngine     │
  │    (range-shared data now visible)           │
  └──────────────────────────────────────────────┘
```

Readers acquire `rsStateMu` with a read lock. This guarantees they either see:
- **Before the commit**: store-local data present, old RSEngine (or none) — correct.
- **After the commit**: store-local data gone (activated clears), new RSEngine
  with the flushed SST — correct.

They never see the intermediate state where store-local data is gone but the
new RSEngine isn't yet installed.

## Concurrency Control Summary

```
  ┌──────────────────────────────────────────────────────────────────┐
  │                    Concurrency Controls                          │
  ├──────────────────────┬───────────────────────────────────────────┤
  │ rangeFlushMu         │ At most one flush per range at a time.    │
  │                      │ Snapshot handoff from application to      │
  │                      │ RangeFlush caller.                        │
  ├──────────────────────┼───────────────────────────────────────────┤
  │ Prepare & Commit:    │ Both latch on RangeDescriptorKey.         │
  │ RangeDescriptorKey   │ Serializes prepare/commit evaluation      │
  │ latch + isAlone      │ order, which determines Raft proposal     │
  │                      │ order. Prevents a second prepare from     │
  │                      │ slipping between a commit's evaluation    │
  │                      │ and application. Mutual exclusion with    │
  │                      │ splits/merges. isAlone ensures clean      │
  │                      │ snapshot boundary (prepare) and atomic    │
  │                      │ activation (commit).                      │
  ├──────────────────────┼───────────────────────────────────────────┤
  │ rsStateMu            │ Atomic swap of RSEngine pointer with      │
  │                      │ store-local state change. Readers never   │
  │                      │ see "data gone, replacement not ready."   │
  ├──────────────────────┼───────────────────────────────────────────┤
  │ FlushStartedCount    │ Replicated counter spanning prepare to    │
  │                      │ commit. The only guard for this window:   │
  │                      │ the latch is per-evaluation, ongoingFlush │
  │                      │ is per-replica. Detects intervening       │
  │                      │ prepares from any leaseholder.            │
  ├──────────────────────┼───────────────────────────────────────────┤
  │ ExpectedDescGen +    │ Guards hardlink-to-evaluation window.     │
  │ MVCC descriptor read │ Detects splits/merges (including          │
  │                      │ uncommitted intents) that would           │
  │                      │ invalidate hardlinks created for a        │
  │                      │ specific replica set.                     │
  ├──────────────────────┼───────────────────────────────────────────┤
  │ ExpectedManifestNum  │ Guards RSEngine-snapshot-to-evaluation    │
  │                      │ window. Detects concurrent compactions    │
  │                      │ that would break the manifest lineage     │
  │                      │ (new manifest is built as a successor     │
  │                      │ to the expected one).                     │
  └──────────────────────┴───────────────────────────────────────────┘
```

## Hazard Examples

### Hazard 1: Concurrent Write During SST Creation

**Scenario**: A write to key `k` arrives between prepare and commit.

```
  Time ──────────────────────────────────────────────────►

  Prepare applies           Write k=v2 applies        Commit applies
  (snapshot taken)          (to store-local)          (activate clears)
       │                         │                         │
       ▼                         ▼                         ▼
  Snapshot sees k=v1        k=v2 in store-local       k=v2 deleted by
  SST contains k=v1        (not in snapshot/SST)      activate clears!
                                                      RSEngine has k=v1
```

**Result**: After commit, the range-shared engine has `k=v1` but the latest
value `k=v2` has been deleted from store-local and is NOT in the SST.

**Why this is safe**: `k=v2` was written *after* the dormant clear. When the
activate fires, Pebble only activates the dormant clear that was written at
prepare time. The write `k=v2` has a higher sequence number than the dormant
clear, so it is NOT shadowed by the activation. The key `k=v2` remains visible
in the store-local engine; only data at or below the dormant clear's sequence
number is deleted.

```
  Store-local Pebble (after commit):
  ┌────────────────────────────────────┐
  │ k=v2  @ seqnum 200                 │ ◄── survives (above dormant)
  │ ACTIVATE [a, z) @ seqnum 300       │
  │ DORMANT  [a, z) @ seqnum 100       │ ◄── shadows k=v1
  │ k=v1  @ seqnum 50                  │ ◄── deleted (below dormant)
  └────────────────────────────────────┘

  Range-shared engine:
  ┌────────────────────────────────────┐
  │ k=v1  (from SST)                   │
  └────────────────────────────────────┘

  Merged read of k: sees k=v2 from store-local
  (k=v1 in range-shared is shadowed by the newer k=v2)
```

### Hazard 2: Splits, Merges, and Compactions Between Prepare and Commit

The prepare-to-commit window can be long (SST creation is off-Raft). During
this window, the range can split and merge multiple times, and compactions can
install new manifests. This is tolerated by the protocol.

**Splits and merges between prepare and commit**:

```
  Time ──────────────────────────────────────────────────────────────►

  Prepare [a,z)       Split at m      Write k=v2    Merge back       Commit
  DORMANT @ seq 100   [a,m) [m,z)     on RHS        to [a,z)         [a,z)
  snapshot taken                       k=v2@seq 150  gen incremented
       │                  │               │              │               │
       ▼                  ▼               ▼              ▼               ▼
  SST has data       Range changes   k=v2 on RHS     [a,z) again    span check ✓
  at seq ≤ 100       independently   at seq > 100     k=v2 in       ACTIVATE
                                                     store-local
```

After activation, the dormant at seqnum 100 deletes store-local data at
seqnums ≤ 100. The SST (in range-shared) provides that data. The write
`k=v2` at seqnum 150 survives in store-local — it's above the dormant
seqnum. Reads merge both engines correctly.

The pre-commit span check (`prepareDesc.StartKey == currentDesc.StartKey`)
catches the case where the range is currently at a different span. If the span
changed, the SST (built for the original span) doesn't cover the current range.
The flush fails and is retried.

**Compactions between prepare and commit**: Compactions install new manifests
via `InstallNewManifest` (with `flushCommit = nil`). This changes the
`ManifestNum`. When the flush commit runs, the RSEngine takes a snapshot of
its own state in `FlushSSTables` and builds the new manifest as a successor
to that snapshot's manifest. `ExpectedManifestNum` is set from this
snapshot, not from the current value at `InstallNewManifest` time. As long as
no further manifest change happens between the RSEngine snapshot and the Raft
command evaluating, the commit succeeds.

**Hazards within the commit phase**: `ExpectedManifestNum` guards the window
from the RSEngine snapshot (in `FlushSSTables`) to Raft evaluation — if a
concurrent compaction changes the manifest in this window, the new manifest
would be built on a stale base, losing the compaction's changes.
`ExpectedDescGeneration` guards the narrower window from hardlink creation
(in `InstallNewManifest`) to Raft evaluation — if a split/merge changes the
replicas in this window, the hardlinks would be for the wrong set of
replicas. The MVCC descriptor read additionally catches uncommitted
split/merge intents.

### Hazard 3: Premature Dormant Activation (Two Prepares)

This is the most subtle hazard and the reason both prepare and commit must latch
on the same key (`RangeDescriptorKey`).

**Scenario**: Flush B's prepare slips into the Raft log between Flush A's
commit evaluation and application.

```
  Time ──────────────────────────────────────────────────────────────►

  Flush A prepare    Write k=v2    Flush A commit     Flush B prepare
  evaluates          arrives       evaluates          evaluates
  (count=5→6)                      (count=6 ✓)        (count=6→7)
       │                │               │                  │
       ▼                ▼               ▼                  ▼
  DORMANT [a,z)    k=v2 @ seq 150  proposed to Raft   proposed to Raft
  @ seq 100                        (Raft entry N+2)   (Raft entry N+1)
  count→6
```

Because Flush B's prepare was proposed first (Raft entry N+1), it applies
before Flush A's commit (Raft entry N+2):

```
  Raft application order:
  ┌─────────────────────────────────────────────────────────────────┐
  │ 1. Flush B prepare applies:                                     │
  │      DORMANT [a,z) @ seq 200                                    │
  │      count: 6 → 7                                               │
  │                                                                 │
  │ 2. Flush A commit applies:                                      │
  │      ACTIVATE [a,z) @ seq 250                                   │
  │      → activates ALL dormants: seq 100 (A) AND seq 200 (B)      │
  │      → k=v2 @ seq 150 < 200 → DELETED by B's dormant!           │
  │      → k=v2 is NOT in Flush A's SST (snapshot predates it)      │
  │      → Flush B hasn't committed → k=v2 not in range-shared      │
  │      → DATA LOST                                                │
  └─────────────────────────────────────────────────────────────────┘
```

The root cause: `ClearRawRangeActivate` activates **all** overlapping dormant
markers, not just the one from the matching prepare. Flush B's dormant at
seqnum 200 gets prematurely activated by Flush A's commit, deleting data
(seqnums 101–200) that isn't in either SST.

**Guard**: The `RangeDescriptorKey` latch on both prepare and commit. Since
both latch on the same key, their evaluations are serialized. On the same
leaseholder, serialized evaluation means serialized Raft proposal order:

```
  With RangeDescriptorKey latch:

  Flush A prepare: acquires latch, evaluates, proposes (Raft N), releases
  Flush A prepare applies: count→6
  Flush A commit:  acquires latch, evaluates (count=6 ✓), proposes (Raft N+k)
  Flush A commit releases latch
  Flush B prepare: acquires latch, evaluates, proposes (Raft N+k+1)

  Application order: ..., Flush A commit (N+k), Flush B prepare (N+k+1)
  → Flush A's activate fires BEFORE Flush B's dormant is written
  → No premature activation possible
```

**Complementary guard**: `FlushStartedCount` guards the prepare-to-commit
window. The latch is scoped to a single evaluation — it is released after
each proposal — so it cannot span from prepare to commit. If Flush B's
prepare applies between Flush A's prepare and commit (e.g., after a lease
transfer to a different node), the count increments from 6 to 7. When
Flush A's commit evaluates, it sees count=7 ≠ expected 6 and fails.

The latch and `FlushStartedCount` are complementary: the latch prevents the
Hazard 3 interleaving (a prepare slipping between a commit's evaluation
and application on the same leaseholder); `FlushStartedCount` detects
intervening prepares across the entire prepare-to-commit window, including
from other leaseholders.

### Hazard 4: RSEngine Swap Visibility

**Scenario**: A read is executing while the commit applies.

```
  Without rsStateMu:
  ┌────────────────────────────────────────────────────────┐
  │ Read begins                                            │
  │   1. Pin store-local state  (sees data)                │
  │                             ┌────────────────────────┐ │
  │                             │ Commit applies:        │ │
  │                             │   Activate clears      │ │
  │                             │   (data gone!)         │ │
  │                             │                        │ │
  │   2. Get RSEngine snapshot  │   Swap RSEngine        │ │
  │      (gets OLD engine!)     │   (new data here)      │ │
  │                             └────────────────────────┘ │
  │   3. Read returns: data missing from BOTH engines!     │
  └────────────────────────────────────────────────────────┘
```

**Guard**: `rsStateMu` prevents this. The commit holds `rsStateMu` in write
mode during the batch commit + RSEngine swap. Readers hold `rsStateMu` in read
mode while pinning store-local state and acquiring the RSEngine snapshot. The
two cannot interleave.

```
  With rsStateMu:
  ┌───────────────────────────────┐
  │ Read (rsStateMu.RLock):       │
  │   1. Pin store-local state    │
  │   2. Get RSEngine snapshot    │ ◄── both from same "era"
  │   3. Read from both           │
  └───────────────────────────────┘
       cannot interleave with
  ┌───────────────────────────────┐
  │ Commit (rsStateMu.Lock):      │
  │   1. Commit batch (activate)  │
  │   2. Swap RSEngine pointer    │ ◄── atomic transition
  └───────────────────────────────┘
```

### Hazard 5: Wrong Snapshot from Cancelled Flush

**Invariant**: The snapshot used to build the SST must correspond to the
prepare whose dormant clears will be activated by the commit. If Flush B
uses a snapshot from Flush A's prepare (taken at A's seqnum), but B's
dormant clears are at B's higher seqnum, the SST will miss writes between
the two seqnums — and those writes will be shadowed when B's dormant clears
are activated.

**Scenario**: Flush A is cancelled after proposing prepare but before its
prepare applies. Flush B starts, and A's `prepareLocalResult` stores the
wrong snapshot into `rangeFlushMu`.

```
  Time ──────────────────────────────────────────────────────────────►

  Flush A proposes      Flush A cancelled       Flush B starts
  prepare               (ongoingFlush → false)  (ongoingFlush → true)
       │                     │                       │
       ▼                     ▼                       ▼
                         deferred cleanup        Flush B proposes
                         clears rangeFlushMu     prepare
       │                                              │
       ▼                                              ▼
  Flush A's prepare applies               Flush B's prepare applies
  (prepareLocalResult runs)               (prepareLocalResult runs)
  ongoingFlush is true (from B!)          stores Flush B's snapshot
  → stores Flush A's snapshot
```

If Flush B reads the snapshot before its own `prepareLocalResult`
overwrites it, it gets A's snapshot — taken at A's prepare seqnum, not
B's. The consequence is data loss:

```
  Flush A's prepare: DORMANT @ seq 100, snapshot S_A taken
  Writes arrive:     k=v2 @ seq 150
  Flush B's prepare: DORMANT @ seq 200, snapshot S_B taken

  If B uses S_A instead of S_B:
  ┌─────────────────────────────────────────────────────────────┐
  │ B's SST contains data at seq ≤ 100 (from S_A)              │
  │ B's dormant clears are at seq 200                           │
  │ B's commit activates dormant @ seq 200                      │
  │   → shadows all store-local data at seq ≤ 200               │
  │   → k=v2 @ seq 150: DELETED (below dormant seq 200)         │
  │   → k=v2 NOT in B's SST (S_A predates it)                   │
  │   → DATA LOST                                               │
  └─────────────────────────────────────────────────────────────┘
```

**Guard**: `RangeFlush` uses `r.AnnotateCtx(context.Background())` — the
context is never cancelled except on Replica removal. If the replica is being
removed, no Flush B can start. This eliminates the interleaving entirely:
Flush A runs to completion (or fails and cleans up) before Flush B can begin,
so `prepareLocalResult` always stores a snapshot for the flush that is
actually in progress.

## Summary

The range flush protocol carefully separates the **cheap replicated work**
(prepare: increment counter, write dormant clears) from the **expensive local
work** (SST creation) and the **critical replicated work** (commit: activate
clears, swap manifest). Each phase has precisely the concurrency control it
needs — no more, no less:

- **Prepare and commit** both latch on `RangeDescriptorKey` and are `isAlone`.
  The shared latch serializes their Raft proposal order, preventing a second
  prepare's dormant from being prematurely activated by the first commit.
  `isAlone` ensures a clean snapshot boundary (prepare) and atomic activation
  (commit).
- **SST creation** runs completely outside Raft and doesn't block anything.
- **`rsStateMu`** makes the RSEngine swap atomic with the store-local state
  change, preventing readers from seeing a gap.
- **Replicated counters and generation checks** catch every form of concurrent
  mutation (other flushes, splits, merges, compactions) that would invalidate
  the SST.
