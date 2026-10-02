# Derived cache retention

Schedule retains at most 128 recently used parsed cron expressions per family.
Each live definition owns its parsed cron independently, so parse-cache eviction
does not change due times or recovery. The bound also applies during preload,
failed persistence and rejected batches.

Stream retains resource actors only while an append session is active. Resource
READ, LAST and METADATA operations use temporary actors when no live append
session exists; completion or disconnect releases the idle actor. Reconstruction
loads committed offsets and data from StreamStore. Read cleanup checks only the
selected actor, without scanning other resources. The active Stream gauge counts
resident actors and falls when their append sessions finish; committed inventory
is separate and remains storage-backed.
