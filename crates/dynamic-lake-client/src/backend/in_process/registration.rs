use std::collections::BTreeSet;
use std::sync::Arc;

use dynamic_lake_core::{
    GraphOperations, MaterializedViewDefinition, PipelineBuildOptions, PipelineError,
    PipelineResult,
};
use dynamic_lake_datafusion::ivm::{canonical_view_id, normalize::materialized_view_schema};

use crate::ViewDefinition;

use super::{InProcessPipelineClient, RegisteredViewSet, metadata_error};

impl InProcessPipelineClient {
    pub(super) async fn load_views(&self) -> PipelineResult<()> {
        let stored = self.stored_definitions().await?;
        if stored.is_empty() {
            return Ok(());
        }
        let mut view_set = RegisteredViewSet::build(
            &self.session,
            stored
                .iter()
                .map(|definition| {
                    (
                        ViewDefinition {
                            name: definition.node_id.to_string(),
                            sql: definition.sql.clone(),
                        },
                        definition.options.clone(),
                    )
                })
                .collect(),
        )
        .await?;
        self.restore_streams(&mut view_set, &stored).await?;
        *self.view_set.write().expect("view set lock poisoned") = Some(Arc::new(view_set));
        Ok(())
    }

    async fn stored_definitions(&self) -> PipelineResult<Vec<MaterializedViewDefinition>> {
        self.metadata
            .list_mv_definitions()
            .await
            .map_err(metadata_error)
    }

    async fn restore_streams(
        &self,
        view_set: &mut RegisteredViewSet,
        stored: &[MaterializedViewDefinition],
    ) -> PipelineResult<()> {
        for definition in stored {
            let signature = view_set
                .plan_signatures
                .get(&definition.node_id)
                .ok_or_else(|| {
                    PipelineError::Invalid(format!("MV `{}` was not rebuilt", definition.node_id))
                })?;
            if signature != &definition.plan_signature {
                return Err(PipelineError::Invalid(format!(
                    "IVM plan signature changed for `{}`; full refresh required",
                    definition.node_id
                )));
            }
            let streams = self
                .metadata
                .list_stream_objects(&definition.node_id)
                .await
                .map_err(metadata_error)?;
            let expected = view_set
                .streams
                .get_mut()
                .get_mut(&definition.node_id)
                .ok_or_else(|| {
                    PipelineError::Invalid(format!("MV `{}` has no streams", definition.node_id))
                })?;
            if expected.len() != streams.len()
                || streams.iter().any(|stream| {
                    stream.mv_node_id != definition.node_id
                        || !expected.contains_key(&stream.source)
                })
            {
                return Err(PipelineError::Invalid(format!(
                    "source streams changed for `{}`; full refresh required",
                    definition.node_id
                )));
            }
            for stream in streams {
                expected.insert(stream.source.clone(), stream);
            }
        }
        Ok(())
    }

    /// TODO: incrementally update MV graph
    /// here we reconstruct the whole graph for simplicity
    pub(super) async fn create_views_impl(
        &self,
        definitions: Vec<ViewDefinition>,
        options: PipelineBuildOptions,
    ) -> PipelineResult<()> {
        let existing = self.stored_definitions().await?;
        let existing_ids = existing
            .iter()
            .map(|definition| definition.node_id.clone())
            .collect::<BTreeSet<_>>();
        let mut new_ids = BTreeSet::new();
        for definition in &definitions {
            let id = canonical_view_id(&definition.name, &options)?;
            if existing_ids.contains(&id) || !new_ids.insert(id.clone()) {
                return Err(PipelineError::Invalid(format!(
                    "materialized view `{id}` already exists"
                )));
            }
        }
        let all_definitions = existing
            .iter()
            .map(|definition| {
                (
                    ViewDefinition {
                        name: definition.node_id.to_string(),
                        sql: definition.sql.clone(),
                    },
                    definition.options.clone(),
                )
            })
            .chain(
                definitions
                    .iter()
                    .cloned()
                    .map(|definition| (definition, options.clone())),
            )
            .collect();
        let mut view_set = RegisteredViewSet::build(&self.session, all_definitions).await?;
        self.restore_streams(&mut view_set, &existing).await?;
        let mut new_metadata = Vec::with_capacity(definitions.len());
        for definition in definitions {
            let id = canonical_view_id(&definition.name, &options)?;
            new_metadata.push(MaterializedViewDefinition::new(
                id.clone(),
                definition.sql,
                options.clone(),
                view_set.plan_signatures[&id].clone(),
            ));
        }
        for view in view_set
            .graph
            .topological_order()
            .iter()
            .filter(|view| new_ids.contains(*view))
        {
            let node = view_set.graph.node(view).expect("planned MV node");
            let catalog = view.parts().first().ok_or_else(|| {
                PipelineError::Invalid(format!("materialized view `{view}` has no catalog"))
            })?;
            let format = self.formats.catalog(catalog).ok_or_else(|| {
                PipelineError::Catalog(format!("no lake format registered for catalog `{catalog}`"))
            })?;
            format
                .create_materialized_view_table(view.clone(), materialized_view_schema(&node.query))
                .await
                .map_err(|error| PipelineError::Catalog(error.to_string()))?;
        }
        for definition in &new_metadata {
            let streams = view_set
                .streams
                .lock()
                .await
                .get(&definition.node_id)
                .expect("planned MV streams")
                .values()
                .cloned()
                .collect::<Vec<_>>();
            self.metadata
                .create_materialized_view(definition, &streams)
                .await
                .map_err(metadata_error)?;
        }
        *self.view_set.write().expect("view set lock poisoned") = Some(Arc::new(view_set));
        Ok(())
    }
}
