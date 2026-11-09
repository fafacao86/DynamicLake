use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use futures::StreamExt;
use paimon::Table;

#[derive(Debug)]
pub(super) struct PaimonChangeSink {
    table: Table,
    schema: SchemaRef,
}

impl PaimonChangeSink {
    pub fn new(table: Table, schema: SchemaRef) -> Self {
        Self { table, schema }
    }
}

impl DisplayAs for PaimonChangeSink {
    fn fmt_as(&self, _type: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "PaimonChangeSink: table={}",
            self.table.identifier()
        )
    }
}

#[async_trait]
impl DataSink for PaimonChangeSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let builder = self.table.new_write_builder();
        let mut writer = builder.new_write().map_err(error)?;
        let mut row_count = 0;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            row_count += batch.num_rows() as u64;
            let fields = batch
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    if field.data_type() == &DataType::Utf8View {
                        Arc::new(field.as_ref().clone().with_data_type(DataType::Utf8))
                    } else {
                        Arc::clone(field)
                    }
                })
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(fields));
            let columns = batch
                .columns()
                .iter()
                .zip(schema.fields())
                .map(|(column, field)| {
                    if column.data_type() == field.data_type() {
                        Ok(Arc::clone(column))
                    } else {
                        datafusion::arrow::compute::cast(column, field.data_type())
                            .map_err(Into::into)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            writer
                .write_arrow_batch(&RecordBatch::try_new(schema, columns)?)
                .await
                .map_err(error)?;
        }
        let messages = writer.prepare_commit().await.map_err(error)?;
        builder
            .try_new_commit()
            .map_err(error)?
            .commit(messages)
            .await
            .map_err(error)?;
        Ok(row_count)
    }
}

fn error(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}
