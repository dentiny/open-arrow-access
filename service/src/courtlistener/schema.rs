//! Schema contract pinned to the exported 2026-09-30 headers and companion SQL.
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use std::sync::{Arc, OnceLock};

/// Field definitions for the four entities in one joined output schema.
/// Source-column order and historical variants must be supplied explicitly.
#[derive(Clone, Debug)]
pub struct CourtListenerSchemas {
    pub opinion: Fields,
    pub cluster: Fields,
    pub docket: Fields,
    pub court: Fields,
}
impl Default for CourtListenerSchemas {
    fn default() -> Self {
        static DEFAULTS: OnceLock<CourtListenerSchemas> = OnceLock::new();
        DEFAULTS.get_or_init(Self::build_default).clone()
    }
}
impl CourtListenerSchemas {
    fn build_default() -> Self {
        Self {
            opinion: vec![
                Field::new("id", DataType::Int64, false),
                Field::new(
                    "date_created",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new(
                    "date_modified",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new("author_str", DataType::Utf8, false),
                Field::new("per_curiam", DataType::Boolean, false),
                Field::new("joined_by_str", DataType::Utf8, false),
                Field::new("type", DataType::Utf8, false),
                Field::new("sha1", DataType::Utf8, false),
                Field::new("page_count", DataType::UInt64, true),
                Field::new("download_url", DataType::Utf8, true),
                Field::new("local_path", DataType::Utf8, false),
                Field::new("plain_text", DataType::Utf8, false),
                Field::new("html", DataType::Utf8, false),
                Field::new("html_lawbox", DataType::Utf8, false),
                Field::new("html_columbia", DataType::Utf8, false),
                Field::new("html_anon_2020", DataType::Utf8, false),
                Field::new("xml_harvard", DataType::Utf8, false),
                Field::new("xml_scan", DataType::Utf8, false),
                Field::new("html_with_citations", DataType::Utf8, false),
                Field::new("extracted_by_ocr", DataType::Boolean, false),
                Field::new("author_id", DataType::Int64, true),
                Field::new("cluster_id", DataType::Int64, false),
            ]
            .into(),
            cluster: vec![
                Field::new("id", DataType::Int64, false),
                Field::new(
                    "date_created",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new(
                    "date_modified",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new("judges", DataType::Utf8, false),
                Field::new("date_filed", DataType::Date32, false),
                Field::new("date_filed_is_approximate", DataType::Boolean, false),
                Field::new("slug", DataType::Utf8, true),
                Field::new("case_name_short", DataType::Utf8, false),
                Field::new("case_name", DataType::Utf8, false),
                Field::new("case_name_full", DataType::Utf8, false),
                Field::new("scdb_id", DataType::Utf8, false),
                Field::new("scdb_decision_direction", DataType::Int32, true),
                Field::new("scdb_votes_majority", DataType::UInt64, true),
                Field::new("scdb_votes_minority", DataType::UInt64, true),
                Field::new("source", DataType::Utf8, false),
                Field::new("procedural_history", DataType::Utf8, false),
                Field::new("attorneys", DataType::Utf8, false),
                Field::new("nature_of_suit", DataType::Utf8, false),
                Field::new("posture", DataType::Utf8, false),
                Field::new("syllabus", DataType::Utf8, false),
                Field::new("headnotes", DataType::Utf8, false),
                Field::new("summary", DataType::Utf8, false),
                Field::new("disposition", DataType::Utf8, false),
                Field::new("history", DataType::Utf8, false),
                Field::new("other_dates", DataType::Utf8, false),
                Field::new("cross_reference", DataType::Utf8, false),
                Field::new("correction", DataType::Utf8, false),
                Field::new("citation_count", DataType::UInt64, false),
                Field::new("precedential_status", DataType::Utf8, false),
                Field::new("date_blocked", DataType::Date32, true),
                Field::new("blocked", DataType::Boolean, false),
                Field::new("filepath_json_harvard", DataType::Utf8, false),
                Field::new("filepath_pdf_harvard", DataType::Utf8, false),
                Field::new("docket_id", DataType::Int64, false),
                Field::new("arguments", DataType::Utf8, false),
                Field::new("headmatter", DataType::Utf8, false),
            ]
            .into(),
            docket: vec![
                Field::new("id", DataType::Int64, false),
                Field::new(
                    "date_created",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new(
                    "date_modified",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new("source", DataType::Int16, false),
                Field::new("appeal_from_str", DataType::Utf8, false),
                Field::new("assigned_to_str", DataType::Utf8, false),
                Field::new("referred_to_str", DataType::Utf8, false),
                Field::new("panel_str", DataType::Utf8, false),
                Field::new(
                    "date_last_index",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Field::new("date_cert_granted", DataType::Date32, true),
                Field::new("date_cert_denied", DataType::Date32, true),
                Field::new("date_argued", DataType::Date32, true),
                Field::new("date_reargued", DataType::Date32, true),
                Field::new("date_reargument_denied", DataType::Date32, true),
                Field::new("date_filed", DataType::Date32, true),
                Field::new("date_terminated", DataType::Date32, true),
                Field::new("date_last_filing", DataType::Date32, true),
                Field::new("case_name_short", DataType::Utf8, false),
                Field::new("case_name", DataType::Utf8, false),
                Field::new("case_name_full", DataType::Utf8, false),
                Field::new("slug", DataType::Utf8, false),
                Field::new("docket_number", DataType::Utf8, true),
                Field::new("docket_number_core", DataType::Utf8, false),
                Field::new("pacer_case_id", DataType::Utf8, true),
                Field::new("cause", DataType::Utf8, false),
                Field::new("nature_of_suit", DataType::Utf8, false),
                Field::new("jury_demand", DataType::Utf8, false),
                Field::new("jurisdiction_type", DataType::Utf8, false),
                Field::new("appellate_fee_status", DataType::Utf8, false),
                Field::new("appellate_case_type_information", DataType::Utf8, false),
                Field::new("mdl_status", DataType::Utf8, false),
                Field::new("filepath_local", DataType::Utf8, false),
                Field::new("filepath_ia", DataType::Utf8, false),
                Field::new("filepath_ia_json", DataType::Utf8, false),
                Field::new("ia_upload_failure_count", DataType::UInt64, true),
                Field::new("ia_needs_upload", DataType::Boolean, true),
                Field::new(
                    "ia_date_first_change",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Field::new("view_count", DataType::UInt64, false),
                Field::new("date_blocked", DataType::Date32, true),
                Field::new("blocked", DataType::Boolean, false),
                Field::new("appeal_from_id", DataType::Utf8, true),
                Field::new("assigned_to_id", DataType::Int64, true),
                Field::new("court_id", DataType::Utf8, false),
                Field::new("idb_data_id", DataType::Int64, true),
                Field::new("originating_court_information_id", DataType::Int64, true),
                Field::new("referred_to_id", DataType::Int64, true),
                Field::new("federal_dn_case_type", DataType::Utf8, false),
                Field::new("federal_dn_office_code", DataType::Utf8, false),
                Field::new("federal_dn_judge_initials_assigned", DataType::Utf8, false),
                Field::new("federal_dn_judge_initials_referred", DataType::Utf8, false),
                Field::new("federal_defendant_number", DataType::Int16, true),
                Field::new("parent_docket_id", DataType::Int64, true),
                Field::new("docket_number_raw", DataType::Utf8, false),
                Field::new("docket_number_source", DataType::Int16, false),
            ]
            .into(),
            court: vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("pacer_court_id", DataType::Int16, true),
                Field::new("pacer_has_rss_feed", DataType::Boolean, true),
                Field::new("pacer_rss_entry_types", DataType::Utf8, false),
                Field::new(
                    "date_last_pacer_contact",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Field::new("fjc_court_id", DataType::Utf8, false),
                Field::new(
                    "date_modified",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new("in_use", DataType::Boolean, false),
                Field::new("has_opinion_scraper", DataType::Boolean, false),
                Field::new("has_oral_argument_scraper", DataType::Boolean, false),
                Field::new("position", DataType::Float64, false),
                Field::new("citation_string", DataType::Utf8, false),
                Field::new("short_name", DataType::Utf8, false),
                Field::new("full_name", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("start_date", DataType::Date32, true),
                Field::new("end_date", DataType::Date32, true),
                Field::new("jurisdiction", DataType::Utf8, false),
                Field::new("notes", DataType::Utf8, false),
                Field::new("parent_court_id", DataType::Utf8, true),
            ]
            .into(),
        }
    }
}
impl CourtListenerSchemas {
    pub(crate) fn tables(&self) -> [&Fields; 4] {
        [&self.opinion, &self.cluster, &self.docket, &self.court]
    }
    /// Fixed nested output schema: one row per opinion, with nullable related structs.
    /// Default callers share one allocation; historical overrides never enter that cache.
    pub fn schema(&self) -> SchemaRef {
        static JOINED: OnceLock<SchemaRef> = OnceLock::new();
        let defaults = Self::default();
        let fields = [&self.opinion, &self.cluster, &self.docket, &self.court];
        let default_fields = [
            &defaults.opinion,
            &defaults.cluster,
            &defaults.docket,
            &defaults.court,
        ];
        if fields
            .into_iter()
            .zip(default_fields)
            .all(|(a, b)| same_fields(a, b))
        {
            JOINED.get_or_init(|| self.build_schema()).clone()
        } else {
            self.build_schema()
        }
    }
    fn build_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("snapshot_date", DataType::Date32, false),
            Field::new("opinion", DataType::Struct(self.opinion.clone()), false),
            Field::new("cluster", DataType::Struct(self.cluster.clone()), true),
            Field::new("docket", DataType::Struct(self.docket.clone()), true),
            Field::new("court", DataType::Struct(self.court.clone()), true),
            Field::new(
                "provenance",
                DataType::Struct(
                    vec![
                        Field::new("opinions_source_uri", DataType::Utf8, false),
                        Field::new("clusters_source_uri", DataType::Utf8, false),
                        Field::new("dockets_source_uri", DataType::Utf8, false),
                        Field::new("courts_source_uri", DataType::Utf8, false),
                        Field::new("opinion_record_index", DataType::UInt64, false),
                        Field::new("cluster_record_index", DataType::UInt64, true),
                        Field::new("docket_record_index", DataType::UInt64, true),
                        Field::new("court_record_index", DataType::UInt64, true),
                    ]
                    .into(),
                ),
                false,
            ),
        ]))
    }
}

fn same_fields(a: &Fields, b: &Fields) -> bool {
    std::ptr::eq(a.as_ref(), b.as_ref())
}
