use crate::structs::table_row::TableRow;
use serde::{Deserialize, Serialize};

use crate::structs::table_schema::TableSchema;

// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResults {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_rows: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    pub job_complete: bool,
    pub schema: Option<TableSchema>,
    pub rows: Option<Vec<TableRow>>,
}
