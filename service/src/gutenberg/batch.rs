use std::sync::Arc;

use arrow_array::{
    ArrayRef, Date32Array, LargeBinaryArray, LargeStringArray, RecordBatch, RecordBatchOptions,
    UInt64Array,
    builder::{
        Int32Builder, LargeBinaryBuilder, LargeListBuilder, ListBuilder, StringBuilder,
        StructBuilder, UInt64Builder,
    },
};
use arrow_schema::{DataType, Field, SchemaRef};
use open_arrow_access_core::Result;

use super::{
    creator_fields, decode, file_fields,
    metadata::{Book, File},
    subject_fields,
};

fn strings() -> ListBuilder<StringBuilder> {
    ListBuilder::new(StringBuilder::new()).with_field(Field::new("item", DataType::Utf8, false))
}

fn append_strings(builder: &mut ListBuilder<StringBuilder>, values: &[String]) {
    for value in values {
        builder.values().append_value(value);
    }
    builder.append(true);
}

fn append_text(builder: &mut StructBuilder, index: usize, value: Option<&str>) {
    builder
        .field_builder::<StringBuilder>(index)
        .unwrap()
        .append_option(value);
}

fn string_list(values: &[String]) -> ArrayRef {
    let mut builder = strings();
    append_strings(&mut builder, values);
    Arc::new(builder.finish())
}

fn creators(book: &Book) -> ArrayRef {
    let fields = creator_fields();
    let values = StructBuilder::new(
        fields.clone(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(StringBuilder::new()),
            Box::new(strings()),
            Box::new(Int32Builder::new()),
            Box::new(Int32Builder::new()),
        ],
    );
    let mut builder =
        ListBuilder::new(values).with_field(Field::new("item", DataType::Struct(fields), false));
    for creator in &book.creator {
        let values = builder.values();
        append_text(values, 0, creator.uri.as_deref());
        append_text(values, 1, creator.name.as_deref());
        append_strings(
            values
                .field_builder::<ListBuilder<StringBuilder>>(2)
                .unwrap(),
            &creator.alias,
        );
        values
            .field_builder::<Int32Builder>(3)
            .unwrap()
            .append_option(creator.birthdate);
        values
            .field_builder::<Int32Builder>(4)
            .unwrap()
            .append_option(creator.deathdate);
        values.append(true);
    }
    builder.append(true);
    Arc::new(builder.finish())
}

fn subjects(book: &Book) -> ArrayRef {
    let fields = subject_fields();
    let values = StructBuilder::from_fields(fields.clone(), book.subject.len());
    let mut builder =
        ListBuilder::new(values).with_field(Field::new("item", DataType::Struct(fields), false));
    for subject in &book.subject {
        append_text(builder.values(), 0, subject.scheme.as_deref());
        append_text(builder.values(), 1, Some(&subject.value));
        builder.values().append(true);
    }
    builder.append(true);
    Arc::new(builder.finish())
}

fn files(files: &[(&File, Vec<u8>)]) -> ArrayRef {
    let fields = file_fields();
    let values = StructBuilder::new(
        fields.clone(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(strings()),
            Box::new(UInt64Builder::new()),
            Box::new(StringBuilder::new()),
            Box::new(LargeBinaryBuilder::new()),
        ],
    );
    let mut builder = LargeListBuilder::new(values).with_field(Field::new(
        "item",
        DataType::Struct(fields),
        false,
    ));
    for (file, data) in files {
        let values = builder.values();
        append_text(values, 0, Some(&file.uri));
        append_strings(
            values
                .field_builder::<ListBuilder<StringBuilder>>(1)
                .unwrap(),
            &file.format,
        );
        values
            .field_builder::<UInt64Builder>(2)
            .unwrap()
            .append_option(file.extent);
        append_text(values, 3, file.modified.as_deref());
        values
            .field_builder::<LargeBinaryBuilder>(4)
            .unwrap()
            .append_value(data);
        values.append(true);
    }
    builder.append(true);
    Arc::new(builder.finish())
}

/// Build only projected columns; raw bytes are copied without text conversion.
pub(super) fn create_batch(
    book: &Book,
    raw: &[u8],
    selected: &[(&File, Vec<u8>)],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| -> ArrayRef {
            match field.name().as_str() {
                "ebook_id" => Arc::new(UInt64Array::from(vec![book.ebook_id])),
                "title" => Arc::new(LargeStringArray::from(vec![book.title.as_deref()])),
                "creator" => creators(book),
                "language" => string_list(&book.language),
                "subject" => subjects(book),
                "bookshelf" => string_list(&book.bookshelf),
                "issued" => Arc::new(Date32Array::from(vec![book.issued])),
                "rights" => Arc::new(LargeStringArray::from(vec![book.rights.as_deref()])),
                "metadata_rdf" => Arc::new(LargeBinaryArray::from(vec![raw])),
                "files" => files(selected),
                _ => unreachable!("projection was resolved against the Gutenberg schema"),
            }
        })
        .collect();
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(|error| decode("could not build Gutenberg batch").with_source(error))
}
