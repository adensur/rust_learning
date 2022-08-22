use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Type {
    #[default]
    Unknown,
    String,
    Integer,
    Int64,
    Float,
    Float64,
    Date,
    Bool,
    Record,
}

// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableFieldSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: Type,
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<TableFieldSchema>>,
}
