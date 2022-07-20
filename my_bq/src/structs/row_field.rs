use serde::{Deserialize, Serialize};

// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RowField {
    #[serde(rename = "v", skip_serializing_if = "Option::is_none")]
    pub row_fields: Option<serde_json::Value>,
}
