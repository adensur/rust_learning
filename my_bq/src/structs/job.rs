use crate::structs::job_configuration::JobConfiguration;
use crate::structs::job_configuration_query::JobConfigurationQuery;
use crate::structs::job_reference::JobReference;
use crate::structs::job_status::JobStatus;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<JobConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_reference: Option<JobReference>,
    pub status: Option<JobStatus>,
}

impl Job {
    pub fn new(query: String) -> Self {
        Job {
            configuration: Some(JobConfiguration {
                query: Some(JobConfigurationQuery {
                    query: Some(query),
                    use_legacy_sql: Some(false),
                }),
            }),
            job_reference: None,
            status: None,
        }
    }
}
