use serde::{Deserialize, Serialize};

use super::table_row::TableRow;

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(untagged)]
pub enum Value {
    #[default]
    Unknown,
    String(String),
    Array(Vec<RowField>),
    Record(TableRow),
}

// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RowField {
    #[serde(rename = "v")]
    pub value: Option<Value>,
}
