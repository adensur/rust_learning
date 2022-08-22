use crate::structs::row_field::RowField;
use serde::{Deserialize, Serialize};

// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TableRow {
    #[serde(rename = "f")]
    pub fields: Vec<RowField>,
}
