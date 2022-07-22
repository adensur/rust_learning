use crate::structs::table_row::TableRow;
use serde::{Deserialize, Serialize};

use crate::structs::table_schema::TableSchema;

// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResults {
    pub total_rows: String,
    pub rows: Vec<TableRow>,
    pub schema: TableSchema,
}
