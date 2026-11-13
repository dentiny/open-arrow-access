use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Fields, Schema};
use open_arrow_access_core::Result;

use super::{error, validation};

#[derive(Default)]
pub(super) struct SchemaUnion {
    fields: FieldUnion,
    metadata: HashMap<String, String>,
}

impl SchemaUnion {
    pub(super) fn merge(&mut self, schema: &Schema) -> Result<()> {
        validation::validate_schema_metadata(&self.metadata, schema.metadata())?;
        self.metadata.extend(schema.metadata().clone());
        self.fields.merge(schema.fields())
    }

    pub(super) fn finish(self) -> Schema {
        Schema::new_with_metadata(self.fields.finish(), self.metadata)
    }
}

#[derive(Default)]
struct FieldUnion {
    fields: Vec<UnionField>,
    // Maps field names to indices in `fields`.
    positions: HashMap<String, usize>,
    // Number of field sets merged at this level; fewer occurrences imply a nullable field.
    inputs: usize,
}

impl FieldUnion {
    fn merge(&mut self, fields: &Fields) -> Result<()> {
        self.inputs += 1;
        for field in fields {
            match self.positions.get(field.name()) {
                Some(&index) => self.fields[index].merge(field)?,
                None => {
                    self.positions
                        .insert(field.name().clone(), self.fields.len());
                    self.fields.push(UnionField::new(field));
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> Fields {
        self.fields
            .into_iter()
            .map(|field| Arc::new(field.finish(Some(self.inputs))))
            .collect()
    }
}

struct UnionField {
    field: Field,
    metadata: HashMap<String, String>,
    data_type: UnionType,
    occurrences: usize,
}

impl UnionField {
    fn new(field: &Field) -> Self {
        Self {
            // Keep Arrow's field flags, but merge types and metadata incrementally.
            field: field
                .clone()
                .with_data_type(DataType::Boolean)
                .with_metadata(HashMap::new()),
            metadata: field.metadata().clone(),
            data_type: UnionType::new(field.data_type()),
            occurrences: 1,
        }
    }

    fn merge(&mut self, field: &Field) -> Result<()> {
        validation::validate_field_names(&self.field, field)?;
        let nullable = self.field.is_nullable()
            || field.is_nullable()
            || matches!(self.data_type, UnionType::Leaf(DataType::Null))
            || field.data_type() == &DataType::Null;
        self.data_type
            .merge(field.data_type())
            .map_err(|source| error::field_union_error(field.name(), source))?;
        // A primitive placeholder avoids rebuilding accumulated nested fields in try_merge.
        self.field
            .try_merge(
                &field
                    .clone()
                    .with_data_type(DataType::Boolean)
                    .with_metadata(HashMap::new()),
            )
            .map_err(error::arrow_error)?;
        for (key, value) in field.metadata() {
            if self
                .metadata
                .get(key)
                .is_some_and(|existing| existing != value)
            {
                return Err(error::field_metadata_conflict(field.name(), key));
            }
            self.metadata.insert(key.clone(), value.clone());
        }
        self.field.set_nullable(nullable);
        self.occurrences += 1;
        Ok(())
    }

    fn finish(self, inputs: Option<usize>) -> Field {
        let nullable =
            self.field.is_nullable() || inputs.is_some_and(|inputs| self.occurrences < inputs);
        self.field
            .with_nullable(nullable)
            .with_data_type(self.data_type.finish())
            .with_metadata(self.metadata)
    }
}

enum UnionType {
    Leaf(DataType),
    Struct(FieldUnion),
    List {
        kind: ListKind,
        item: Box<UnionField>,
    },
}

#[derive(Clone, Copy, PartialEq)]
enum ListKind {
    List,
    LargeList,
    ListView,
    LargeListView,
    FixedSizeList(i32),
}

impl ListKind {
    fn from_type(data_type: &DataType) -> Option<(Self, &Field)> {
        Some(match data_type {
            DataType::List(item) => (Self::List, item),
            DataType::LargeList(item) => (Self::LargeList, item),
            DataType::ListView(item) => (Self::ListView, item),
            DataType::LargeListView(item) => (Self::LargeListView, item),
            DataType::FixedSizeList(item, size) => (Self::FixedSizeList(*size), item),
            _ => return None,
        })
    }

    fn finish(self, item: Field) -> DataType {
        let item = Arc::new(item);
        match self {
            Self::List => DataType::List(item),
            Self::LargeList => DataType::LargeList(item),
            Self::ListView => DataType::ListView(item),
            Self::LargeListView => DataType::LargeListView(item),
            Self::FixedSizeList(size) => DataType::FixedSizeList(item, size),
        }
    }
}

impl UnionType {
    fn new(data_type: &DataType) -> Self {
        if let DataType::Struct(fields) = data_type {
            let mut union = FieldUnion::default();
            union.merge(fields).expect("a new field has no conflicts");
            Self::Struct(union)
        } else if let Some((kind, item)) = ListKind::from_type(data_type) {
            Self::List {
                kind,
                item: Box::new(UnionField::new(item)),
            }
        } else {
            Self::Leaf(data_type.clone())
        }
    }

    fn merge(&mut self, data_type: &DataType) -> Result<()> {
        if data_type == &DataType::Null {
            return Ok(());
        }
        if matches!(self, Self::Leaf(DataType::Null)) {
            *self = Self::new(data_type);
            return Ok(());
        }
        match (self, data_type) {
            (Self::Struct(union), DataType::Struct(fields)) => union.merge(fields),
            (Self::List { kind, item }, other)
                if ListKind::from_type(other).is_some_and(|(other, _)| *kind == other) =>
            {
                item.merge(ListKind::from_type(other).unwrap().1)
            }
            (Self::Leaf(left), right) if left == right => Ok(()),
            (left, right) => {
                // Materialize the accumulated type only to explain an incompatible input.
                Err(error::incompatible_types(&left.as_data_type(), right))
            }
        }
    }

    fn as_data_type(&self) -> DataType {
        // Only used on an error path; finishing successful unions consumes their state.
        match self {
            Self::Leaf(data_type) => data_type.clone(),
            Self::Struct(union) => DataType::Struct(
                union
                    .fields
                    .iter()
                    .map(|field| {
                        Arc::new(
                            field
                                .field
                                .clone()
                                .with_data_type(field.data_type.as_data_type()),
                        )
                    })
                    .collect(),
            ),
            Self::List { kind, item } => kind.finish(
                item.field
                    .clone()
                    .with_data_type(item.data_type.as_data_type()),
            ),
        }
    }

    fn finish(self) -> DataType {
        match self {
            Self::Leaf(data_type) => data_type,
            Self::Struct(union) => DataType::Struct(union.finish()),
            Self::List { kind, item } => kind.finish(item.finish(None)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, false)
    }

    fn structure(names: &[&str]) -> DataType {
        DataType::Struct(
            names
                .iter()
                .map(|name| field(name, DataType::Int64))
                .collect(),
        )
    }

    fn merge(schemas: impl IntoIterator<Item = Schema>) -> Schema {
        let mut union = SchemaUnion::default();
        for schema in schemas {
            union.merge(&schema).unwrap();
        }
        union.finish()
    }

    #[test]
    fn missing_and_null_parents_do_not_make_present_children_nullable() {
        let present = Schema::new(vec![field("parent", structure(&["required"]))]);
        let absent = Schema::empty();
        let null = Schema::new(vec![field("parent", DataType::Null)]);
        for schemas in [
            vec![absent.clone(), present.clone(), null.clone()],
            vec![null, present, absent],
        ] {
            let schema = merge(schemas);
            assert!(schema.field(0).is_nullable());
            let DataType::Struct(children) = schema.field(0).data_type() else {
                panic!("expected struct");
            };
            assert!(!children[0].is_nullable());
        }
    }

    #[test]
    fn list_variants_track_missing_children_when_they_reappear() {
        for kind in [
            ListKind::List,
            ListKind::LargeList,
            ListKind::ListView,
            ListKind::LargeListView,
            ListKind::FixedSizeList(2),
        ] {
            let schema = merge(
                [["x"].as_slice(), ["y"].as_slice(), ["x", "y"].as_slice()].map(|names| {
                    Schema::new(vec![field(
                        "items",
                        kind.finish(field("item", structure(names))),
                    )])
                }),
            );
            let (_, item) = ListKind::from_type(schema.field(0).data_type()).unwrap();
            assert!(!item.is_nullable());
            let DataType::Struct(children) = item.data_type() else {
                panic!("expected struct");
            };
            assert_eq!(
                children
                    .iter()
                    .map(|field| (field.name().as_str(), field.is_nullable()))
                    .collect::<Vec<_>>(),
                [("x", true), ("y", true)]
            );
        }
    }

    #[test]
    fn merges_metadata_and_rejects_dictionary_order_conflicts() {
        let metadata = |value: &str| HashMap::from([("key".into(), value.into())]);
        let dictionary = || {
            field(
                "value",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            )
        };
        let mut union = SchemaUnion::default();
        union
            .merge(&Schema::new_with_metadata(
                vec![dictionary().with_metadata(metadata("field"))],
                metadata("schema"),
            ))
            .unwrap();
        union
            .merge(&Schema::new_with_metadata(
                vec![dictionary()],
                HashMap::from([("extra".into(), "extra".into())]),
            ))
            .unwrap();
        let schema = union.finish();
        assert_eq!(schema.metadata()["key"], "schema");
        assert_eq!(schema.metadata()["extra"], "extra");
        assert_eq!(schema.field(0).metadata()["key"], "field");
        assert!(!schema.field(0).is_nullable());
        let mut union = SchemaUnion::default();
        union.merge(&Schema::new(vec![dictionary()])).unwrap();
        assert!(
            union
                .merge(&Schema::new(vec![dictionary().with_dict_is_ordered(true)]))
                .is_err()
        );
    }
}
