use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "UPPERCASE")]
pub enum State {
    Pending,
    Running,
    Done,
}

// https://cloud.google.com/bigquery/docs/reference/rest/v2/JobReference
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<State>,
}
