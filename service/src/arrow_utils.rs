//! Shared Arrow field access without provider-specific errors or empty-value policy.
use arrow_array::{Array, GenericStringArray, OffsetSizeTrait, StructArray};

/// Find a struct child with the expected array type.
/// Missing fields and type mismatches are left to the caller's error policy.
pub(crate) fn struct_field<'a, T: Array + 'static>(
    entity: &'a StructArray,
    field: &str,
) -> Option<&'a T> {
    entity.column_by_name(field)?.as_any().downcast_ref()
}

/// Read a nullable string, preserving empty strings and the original text.
pub(crate) fn string_value<O: OffsetSizeTrait>(
    array: &GenericStringArray<O>,
    row: usize,
) -> Option<&str> {
    (!array.is_null(row)).then(|| array.value(row))
}

/// Read a struct child's string, honoring both parent and child validity.
/// The child must belong to the parent and use the same row coordinates.
pub(crate) fn struct_string_value<'a, O: OffsetSizeTrait>(
    entity: &StructArray,
    array: &'a GenericStringArray<O>,
    row: usize,
) -> Option<&'a str> {
    if entity.is_null(row) {
        None
    } else {
        string_value(array, row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, LargeStringArray, StringArray};
    use arrow_schema::Field;
    use std::sync::Arc;

    #[test]
    fn typed_fields_and_parent_nulls_are_distinct_from_child_values() {
        let array = Arc::new(StringArray::from(vec![
            Some("hidden"),
            Some(""),
            None,
            Some("文本"),
        ]));
        let entity = StructArray::new(
            vec![Arc::new(Field::new(
                "text",
                array.data_type().clone(),
                true,
            ))]
            .into(),
            vec![array],
            Some(vec![false, true, true, true].into()),
        );
        let values = struct_field::<StringArray>(&entity, "text").unwrap();
        assert_eq!(string_value(values, 0), Some("hidden"));
        assert_eq!(struct_string_value(&entity, values, 0), None);
        assert_eq!(struct_string_value(&entity, values, 1), Some(""));
        assert_eq!(struct_string_value(&entity, values, 2), None);
        assert_eq!(struct_string_value(&entity, values, 3), Some("文本"));
        assert!(struct_field::<StringArray>(&entity, "missing").is_none());
        assert!(struct_field::<Int64Array>(&entity, "text").is_none());
        let sliced = entity.slice(1, 3);
        let values = struct_field::<StringArray>(&sliced, "text").unwrap();
        assert_eq!(struct_string_value(&sliced, values, 0), Some(""));
        assert_eq!(struct_string_value(&sliced, values, 1), None);
        assert_eq!(struct_string_value(&sliced, values, 2), Some("文本"));
    }

    #[test]
    fn large_strings_preserve_nulls_empty_text_and_slices() {
        let values = LargeStringArray::from(vec![None, Some(""), Some("λ\ntext")]);
        assert_eq!(string_value(&values, 0), None);
        assert_eq!(string_value(&values, 1), Some(""));
        assert_eq!(string_value(&values.slice(2, 1), 0), Some("λ\ntext"));
    }
}
