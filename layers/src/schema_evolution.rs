use std::{collections::HashSet, sync::Arc};

use arrow_schema::{DataType, SchemaRef};
use datafusion::logical_expr::Expr;
use open_arrow_access_core::{
    Accessor, Layer, ReadOptionsProvider, RecordCapability, RecordReader, Result, async_trait,
};

use crate::validation;
use plan::SchemaEvolutionPlan;

mod execute;
mod plan;

/// One ordered change to the current top-level schema.
/// Names refer to the schema produced by preceding vector items.
#[derive(Clone, Debug)]
pub enum ColumnEvolution {
    AddColumn {
        name: String,
        data_type: DataType,
        expression: Expr,
    },
    RemoveColumn {
        name: String,
    },
    RenameColumn {
        source_name: String,
        target_name: String,
    },
    ChangeDataType {
        name: String,
        data_type: DataType,
    },
}

impl ColumnEvolution {
    fn name(&self) -> &str {
        match self {
            Self::AddColumn { name, .. }
            | Self::RemoveColumn { name }
            | Self::RenameColumn {
                source_name: name, ..
            }
            | Self::ChangeDataType { name, .. } => name,
        }
    }

    /// Append a nullable column computed from the current schema. The expression
    /// must produce the declared type; scalar results expand to the batch row count.
    pub fn add_column(name: impl Into<String>, data_type: DataType, expression: Expr) -> Self {
        Self::AddColumn {
            name: name.into(),
            data_type,
            expression,
        }
    }

    pub fn remove_column(name: impl Into<String>) -> Self {
        Self::RemoveColumn { name: name.into() }
    }

    pub fn rename_column(source_name: impl Into<String>, target_name: impl Into<String>) -> Self {
        Self::RenameColumn {
            source_name: source_name.into(),
            target_name: target_name.into(),
        }
    }

    pub fn change_data_type(name: impl Into<String>, data_type: DataType) -> Self {
        Self::ChangeDataType {
            name: name.into(),
            data_type,
        }
    }
}

/// Add, remove, rename, and cast top-level columns with a fixed output schema.
///
/// Changes run in vector order. Expressions see preceding additions, removals,
/// renames, and casts, even when omitted by the final projection.
/// Existing columns retain their order; additions append to the current schema.
#[derive(Clone, Debug, Default)]
pub struct SchemaEvolutionLayer {
    changes: Arc<[ColumnEvolution]>,
    source_schema: Option<SchemaRef>,
}

impl SchemaEvolutionLayer {
    /// Validate declarations without I/O. Source-dependent checks happen at open.
    pub fn try_new(changes: Vec<ColumnEvolution>) -> Result<Self> {
        validation::validate_evolution_specs(&changes)?;
        Ok(Self {
            changes: changes.into(),
            source_schema: None,
        })
    }

    /// Supply the complete source schema to push dependencies down without
    /// opening another reader for schema discovery.
    pub fn with_source_schema(mut self, schema: SchemaRef) -> Self {
        self.source_schema = Some(schema);
        self
    }
}

/// A record accessor decorated by [`SchemaEvolutionLayer`].
#[derive(Clone, Debug)]
pub struct SchemaEvolutionAccessor<A> {
    inner: A,
    config: SchemaEvolutionLayer,
}

impl<A> Layer<A> for SchemaEvolutionLayer
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Layered = SchemaEvolutionAccessor<A>;
    fn layer(&self, inner: A) -> Self::Layered {
        SchemaEvolutionAccessor {
            inner,
            config: self.clone(),
        }
    }
}

#[async_trait]
impl<A> Accessor for SchemaEvolutionAccessor<A>
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }

    async fn open(&self, mut request: A::Request) -> Result<Box<dyn RecordReader>> {
        let SchemaEvolutionLayer {
            changes,
            source_schema,
        } = &self.config;
        if changes.is_empty() {
            return self.inner.open(request).await;
        }
        let output_options = request.read_options().clone();
        let planned = source_schema
            .as_ref()
            .map(|source| SchemaEvolutionPlan::new(changes, source, &output_options, true))
            .transpose()?;
        // A supplied schema separates namespace checks from source data reads.
        if let Some(plan) = &planned {
            request.read_options_mut().projection = Some(plan.source_columns());
        } else if changes.iter().any(|change| match change {
            ColumnEvolution::ChangeDataType { .. } => false,
            ColumnEvolution::RenameColumn {
                source_name,
                target_name,
            } => source_name != target_name,
            _ => true,
        }) {
            request.read_options_mut().projection = None;
        } else if let Some(columns) = &mut request.read_options_mut().projection {
            let mut names: HashSet<_> = columns.iter().cloned().collect();
            for change in changes.iter() {
                let name = change.name();
                if names.insert(name.to_owned()) {
                    columns.push(name.to_owned());
                }
            }
        }
        let inner = self.inner.open(request).await?;
        let source = &inner.metadata().schema;
        let plan = match planned {
            Some(plan) => plan,
            None => SchemaEvolutionPlan::new(changes, source, &output_options, false)?,
        };
        plan.into_reader(inner)
    }
}
