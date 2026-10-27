# Recoverable snapshot installation

KV data and Raft metadata live in separate RocksDB databases; two successful writes are not one atomic transaction. Installation now follows a redo protocol:

1. Parse the entire KV snapshot and validate replacement entries, including the reserved applied-marker rule and database iterator status.
2. Durably write raftkv-install-intent-v1 in the Raft database. Its record is the 16-byte big-endian snapshot index/term followed by the validated snapshot bytes. Preserve the old Raft tail in the same batch. Do not mutate KV if intent persistence fails.
3. Durably replace KV contents and its applied marker in one KV batch.
4. Durably install Raft snapshot metadata/data and the correct retained suffix/tail, deleting the intent in the same Raft batch.
5. Only then acknowledge completion.

On restart, load the old log/hard state and inspect the intent before checking KV/log index consistency. Replay KV idempotently at the recorded index, then finalize Raft metadata. The pending term must not exceed durable currentTerm. Corrupt metadata, malformed data, backwards KV replacement or storage errors fail closed. Errors still require fail-stop; do not catch them and continue serving consensus operations. A failed recovery retains its intent for a later attempt. A completed recovery removes it, so a second restart does not rewrite KV.

Do not manually remove pending intents or downgrade while an intent exists. Older binaries do not implement recovery and can reject the intermediate state. Back up both databases together before version changes. This does not repair databases already left inconsistent by an older binary without a redo record.

The existing 32 MiB snapshot cap is unchanged. Oversized automatic snapshots (issue #62) remain separate work; this patch addresses issue #64 only.

Tests compile the production consensus/storage sources against in-process transport and RocksDB contract doubles. Fault cases cover failure before KV replacement, after durable KV commit, an ambiguous KV completion, repeated recovery failures, invalid/reserved-marker payloads, second restart, and subsequent replication. All 11 portable suites pass locally. These checks model atomic synchronous database-write semantics; they are not real RocksDB WAL, filesystem fsync, physical power-loss or production Linux-cluster proof. Repository CI and real-store verification remain required before calling the change deployed.
