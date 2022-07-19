use crate::structs::job_configuration_query::JobConfigurationQuery;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<JobConfigurationQuery>,
}
