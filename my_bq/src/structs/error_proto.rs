use serde::{Deserialize, Serialize};

// https://cloud.google.com/bigquery/docs/reference/rest/v2/JobReference
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorProto {
    pub reason: String,
    pub location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub debug_info: Option<String>,
    pub message: String,
}
