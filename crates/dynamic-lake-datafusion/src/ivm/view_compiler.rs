use std::collections::{BTreeMap, BTreeSet};

use datafusion::common::TableReference;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::ObjectName;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use dynamic_lake_core::{
    PipelineBuildOptions, PipelineError, PipelineGraph, PipelineResult, TableIdentifier,
    ViewDefinition, build_view_graph,
};
use dynamic_lake_format::LakeFormatRegistry;

use crate::DataFusionPlanOptimizer;

use super::{IvmNormalizer, IvmPlanSignatureGenerator};

/// Compile SQL definitions into a dependency graph and signatures of its plans.
pub async fn compile_view_definitions(
    session: &SessionContext,
    formats: &LakeFormatRegistry,
    definitions: Vec<(ViewDefinition, PipelineBuildOptions)>,
) -> PipelineResult<(PipelineGraph, BTreeMap<TableIdentifier, String>)> {
    let optimizer = DataFusionPlanOptimizer::new(session.state());
    let mut drafts = Vec::new();
    for (definition, options) in definitions {
        let query = session
            .state()
            .create_logical_plan(&definition.sql)
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))?;
        let optimized = optimizer.optimize(query)?;
        let normalized = IvmNormalizer::default()
            .normalize(optimized, formats)
            .await
            .map_err(|error| PipelineError::Invalid(format!("IVM normalize failed: {error}")))?;
        drafts.push(prepare_table(definition.name, normalized.plan, &options)?);
    }
    let plan_signatures = drafts
        .iter()
        .map(|(id, query, _)| (id.clone(), IvmPlanSignatureGenerator.generate(query).sha256))
        .collect();
    Ok((build_view_graph(drafts)?, plan_signatures))
}

fn prepare_table(
    name: String,
    query: LogicalPlan,
    options: &PipelineBuildOptions,
) -> PipelineResult<(TableIdentifier, LogicalPlan, BTreeSet<TableIdentifier>)> {
    let id = canonical_view_id(&name, options)?;
    let mut references = BTreeSet::new();
    query
        .apply(&mut |node: &LogicalPlan| {
            if let LogicalPlan::TableScan(scan) = node {
                let identifier =
                    table_reference_to_identifier(&scan.table_name).map_err(|error| {
                        datafusion::error::DataFusionError::External(Box::new(error))
                    })?;
                references.insert(identifier);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(|error| PipelineError::Invalid(error.to_string()))?;
    Ok((
        id,
        query,
        references
            .into_iter()
            .map(|reference| options.canonical_table(reference.parts()))
            .collect(),
    ))
}

pub fn canonical_view_id(
    name: &str,
    options: &PipelineBuildOptions,
) -> PipelineResult<TableIdentifier> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(name)
        .map_err(|error| PipelineError::Invalid(format!("invalid table name `{name}`: {error}")))?;
    let object = parser
        .parse_object_name(false)
        .map_err(|error| PipelineError::Invalid(format!("invalid table name `{name}`: {error}")))?;
    if parser.peek_token().token != Token::EOF {
        return Err(PipelineError::Invalid(format!(
            "invalid table name `{name}`: trailing tokens"
        )));
    }
    let parts = object_name_parts(&object)?;
    Ok(options.canonical_table(&parts))
}

fn object_name_parts(name: &ObjectName) -> PipelineResult<Vec<String>> {
    name.0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|identifier| identifier.value.clone())
                .ok_or_else(|| {
                    PipelineError::Invalid(
                        "dynamic table name must not contain identifier functions".to_string(),
                    )
                })
        })
        .collect()
}

fn table_reference_to_identifier(reference: &TableReference) -> PipelineResult<TableIdentifier> {
    let parts = match reference {
        TableReference::Bare { table } => vec![table.to_string()],
        TableReference::Partial { schema, table } => vec![schema.to_string(), table.to_string()],
        TableReference::Full {
            catalog,
            schema,
            table,
        } => vec![catalog.to_string(), schema.to_string(), table.to_string()],
    };
    TableIdentifier::try_new(parts)
}
