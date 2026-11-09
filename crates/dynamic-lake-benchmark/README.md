# Dynamic Lake benchmarks

Run the OJV end-to-end suite with:

```sh
cargo run --release -p dynamic-lake-benchmark -- ojv 1000 100
```

The arguments are the customer count and the number of new orders on the right
side of a left join. `tools::tpch_dataset` uses `tpchgen` to generate customer and
order keys. The suite creates temporary Paimon tables with Parquet data files,
including two managed MVs with deletion vectors and forced L0 compaction.
Their definitions live in `sql/ojv.sql` and `sql/ojv_downstream.sql`.

The suite initializes `mv`, initializes `mv_downstream` from it, then appends
source orders. It applies the source-to-MV signed delta to `mv`, then applies
the MV-to-MV signed delta to `mv_downstream`. Both persisted views are compared
with full SQL recomputation. The native Delta reader supplies downstream
changes after the upstream MV commit. It prints both incremental durations.
The temporary warehouse is removed when the process exits.

The full recomputation refreshes a DataFusion in-memory table from Paimon
before running SQL. The incremental duration includes planning and execution.
