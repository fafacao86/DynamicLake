# Dynamic Lake benchmarks

Run the OJV end-to-end suite with:

```sh
cargo run --release -p dynamic-lake-benchmark -- ojv 1000 100
```

The arguments are the base row count and the number of inserts into the right
side of a left join. The suite creates temporary Paimon tables, builds a view
through `PipelineClient`, plans a source interval from a stream cursor, executes
the native incremental scan, and compares its signed output with a full SQL
recomputation. It prints both durations. The temporary warehouse is removed
when the process exits.

The full recomputation refreshes a DataFusion in-memory table from Paimon
before running SQL. The incremental duration includes planning and execution.
