use crate::structs::row_field::RowField;
use serde::{Deserialize, Serialize};

// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableRow {
    #[serde(rename = "f", skip_serializing_if = "Option::is_none")]
    pub value: Option<Vec<RowField>>,
}
