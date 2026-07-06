# Design: Custom seed-schema override for cold-start binlog reprocessing (MySQL CDC)

- **Date:** 2026-07-06
- **Module:** `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc`
- **Delivery:** Internal fork of flink-cdc (surgical change; consumed by cdh-parent's `debezium-source-connector`)
- **Status:** Approved design — ready for implementation plan

## Problem

A cold-start binlog read (`earliest-offset`, `timestamp`, or `specific-offset` with no Flink
state) seeds its decoding schema from live `SHOW CREATE TABLE` — i.e. the **current** schema. If a
DDL (e.g. `ADD COLUMN`) ran *after* the chosen start offset, the connector tries to decode pre-DDL
rows with a post-DDL schema and crash-loops:

```
SchemaOutOfSyncException / ConnectException: Data row is smaller than a column index,
internal schema representation is probably out of sync with real database schema
```

MySQL cannot answer "schema as of a past offset," so the connector cannot recover on its own. The
only zero-loss path today is snapshot/checkpoint restore (schema comes from Flink state, aligned
with the resume offset). For a historical reprocess with **no pre-DDL snapshot**, there is no
supported recovery.

## Goal

Let an operator who **knows the old schema** supply it as `CREATE TABLE` DDL, so that on a cold
binlog start the decoder seeds from that DDL instead of `SHOW CREATE TABLE`. Pre-DDL rows then
decode correctly; the `ALTER` already present in the binlog is picked up going forward by the
existing streaming DDL-processing (unchanged). This makes cold-start cross-DDL reprocessing
possible without a snapshot.

### Non-goals

- No change to snapshot, `initial`, hybrid, or checkpoint/savepoint **restore** paths.
- No automatic historical-schema reconstruction (that is the separate "externalized schema
  history" idea; out of scope).
- No CDH-side (`cdh-parent`) wiring in this patch — only the flink-cdc surface. The consumer change
  is noted for follow-up.

## Approach

The mechanism already exists; we redirect one call.

- **Seed today (cold binlog start):** `MySqlSourceReader.discoverTableSchemasForBinlogSplit`
  (`MySqlSourceReader.java:465`) sees `split.getTableSchemas().isEmpty()` and calls
  `TableDiscoveryUtils.discoverSchemaForCapturedTables` → `MySqlSchema.getTableSchema` →
  `SHOW CREATE TABLE` (`MySqlSchema.java:80`).
- **DDL → TableChange already exists:** `MySqlSchema.parseSchemaByDdl` (`MySqlSchema.java:118`)
  turns a DDL string into the exact `TableChange` the seed map needs, via
  `databaseSchema.parseSnapshotDdl` — the same path `SHOW CREATE TABLE` output flows through.
- **Serialization confirmed:** the seeded `tableSchemas` is persisted into every checkpoint
  (`MySqlSplitSerializer.writeTableSchemas`, `MySqlSplitSerializer.java:106`) and read back on
  restore, so a correctly-seeded cold start also produces a reusable pre-DDL snapshot as a bonus.

So the change is: an optional per-table seed-schema override that, when present on a cold binlog
start, builds the seed `TableChange` from operator DDL rather than `SHOW CREATE TABLE`.

## Public API

New builder method on `MySqlSourceBuilder`:

```java
public MySqlSourceBuilder<T> seedSchemas(Map<String, String> seedSchemasByTable)
```

- **Key:** `"db.table"` (fully-qualified). **Value:** the old `CREATE TABLE ...` DDL.
- Stored as `Map<String,String>` (not `Map<TableId,String>`) to avoid serialization concerns in
  `MySqlSourceConfig`; parsed to `TableId` at the use site.
- Optional; default empty map ⇒ **behavior identical to today**.

Plumbing: `MySqlSourceBuilder.seedSchemas` → new field + setter on `MySqlSourceConfigFactory`
(alongside e.g. `chunkKeyColumns`) → new field on `MySqlSourceConfig` with `getSeedSchemas()`
(passed through `MySqlSourceConfigFactory.createConfig` / `new MySqlSourceConfig(...)`).

## Component changes

### 1. `MySqlSchema` — public DDL-to-schema entry point
Add:
```java
public TableChange parseTableSchema(MySqlPartition partition, TableId tableId, String createTableDdl)
```
Delegates to the existing private `parseSchemaByDdl`. No new parsing logic; identical `TableChange`
shape as the `SHOW CREATE TABLE` path.

### 2. `TableDiscoveryUtils` — per-table seed selection
In the `discoverSchemaForCapturedTables(partition, sourceConfig, jdbc)` overload (used at
`MySqlSourceReader.java:474`), build the per-table map so that, for each captured `TableId`:
- if `sourceConfig.getSeedSchemas()` has an entry for it → `mySqlSchema.parseTableSchema(...)` from
  the supplied DDL;
- else → existing `SHOW CREATE TABLE` path (**per-table fallback**).

This is the only behavioral branch. The restore path never reaches here (`getTableSchemas()`
non-empty), so checkpoints/savepoints are untouched.

### 3. `MySqlSourceConfig` / `MySqlSourceConfigFactory` / `MySqlSourceBuilder`
Thread the optional `Map<String,String>` through, defaulting to empty.

## Guardrails

- Override is only ever consulted when seeding an **empty** binlog split (cold start). Structurally
  cannot affect restore.
- **WARN at startup** if `seedSchemas` is non-empty while `startupOptions` is `initial`/snapshot-based
  — it will not take effect there, and this most likely signals misconfiguration. (WARN is the
  minimum log level per CDH conventions and this carries operational-alert value.)
- Map keys that do not match any captured table → **WARN and ignore** (typo protection).

## Testing (TDD — tests written first)

1. **Unit — `MySqlSchema.parseTableSchema`:** for a given `CREATE TABLE` DDL, the produced
   `TableChange` is structurally equal to the one produced from the same DDL via the
   `SHOW CREATE TABLE` code path (columns, types, PK).
2. **Unit — `TableDiscoveryUtils` seed selection:** with a mocked JDBC/`MySqlSchema`, an overridden
   table uses the supplied DDL and a non-overridden captured table falls back to the JDBC path.
3. **Integration (the real proof) — testcontainers MySQL:**
   - create table → insert N rows → `ALTER TABLE ... ADD COLUMN` → insert more rows;
   - start `MySqlSource` at `earliest`/`timestamp` **with** the old `CREATE TABLE` in `seedSchemas`;
   - assert: no `SchemaOutOfSyncException`; pre-`ALTER` rows decode; post-`ALTER` rows carry the new
     column (forward DDL pickup works).
4. **Integration — negative control:** the same scenario **without** the override still reproduces
   the crash. Guards against silently losing the reason the fix exists. *(Confirmed keep, per
   review.)*

## Downstream follow-up (not in this patch)

`CdhDebeziumBinlogSource.createSingleSourceFromConfig` gains a `.seedSchemas(...)` call fed from a
new optional per-source config field (e.g. `seed.schema.ddl` map in the source YAML). Tracked
separately in cdh-parent.

## Risks

- **Wrong DDL supplied:** operator provides a schema that does not match the rows at the start
  offset → same class of decode error. Mitigation: the per-table fallback and the negative-control
  test make the failure mode explicit; documentation must stress "DDL must be the schema in effect
  *at the start offset*."
- **Version-bump maintenance:** internal fork carries the patch across flink-cdc upgrades. Change is
  small and localized to reduce merge cost.

## Operational caveats (from final review)

- **A wrong seed is baked into checkpoint state and cannot be corrected by re-editing `seedSchemas`.**
  A seeded cold start writes the seeded `TableChange` into the binlog split's `tableSchemas`, which is
  persisted to every checkpoint (`MySqlSplitSerializer.writeTableSchemas`). On restore the connector
  reads the schema from state and does **not** re-consult `seedSchemas`. So if the supplied DDL was
  wrong, editing `seedSchemas` and restoring will not fix it — you must discard state and do a fresh
  cold start. Upside: a *correct* seeded cold start gives you a reusable pre-DDL snapshot for free.
  Operators must get the DDL right on the first cold start.
- **Seed keys must match the server's normalized `db.table` casing.** `seedKey` is
  `tableId.catalog() + "." + tableId.table()`. On a case-insensitive server, `TableId` may be
  normalized, so a key with different casing silently misses → per-table fallback to
  `SHOW CREATE TABLE` → the original crash. The unknown-key WARN partially surfaces this. Document
  that keys must match the normalized casing.
- **Do not combine `seedSchemas` with `scanNewlyAddedTableEnabled` expecting the seed to apply to
  newly-added tables.** By design the seed applies only on cold start; tables discovered mid-stream
  are read from live `SHOW CREATE TABLE` (see `discoverSeededSchemaForCapturedTables` vs the pure
  `discoverSchemaForCapturedTables` overload).
