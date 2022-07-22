use serde::{Deserialize, Serialize};

use crate::structs::table_field_schema::TableFieldSchema;

// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSchema {
    pub fields: Vec<TableFieldSchema>,
}
