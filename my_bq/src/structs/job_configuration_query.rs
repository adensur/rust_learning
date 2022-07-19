use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobConfigurationQuery {
    // Actual SQL query text
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    // Changes syntax of SQL query. See https://cloud.google.com/bigquery/docs/reference/legacy-sql for details
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_legacy_sql: Option<bool>,
}
