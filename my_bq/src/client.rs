use std::fmt;
use std::sync::Arc;

use crate::error::BigQueryError;
use crate::structs;

use yup_oauth2::authenticator::DefaultAuthenticator;

#[derive(Clone)]
struct InnerClient {
    authenticator: DefaultAuthenticator,
    reqwest_client: reqwest::Client,
}
pub struct Client {
    inner_client: Arc<InnerClient>,
}

const SCOPES: &[&str; 1] = &["https://www.googleapis.com/auth/bigquery"];

impl Client {
    pub async fn new() -> Self {
        let secret = yup_oauth2::read_authorized_user_secret(
            "/Users/mgaiduk/.config/gcloud/application_default_credentials.json",
        )
        .await
        .unwrap();
        let authenticator = yup_oauth2::AuthorizedUserAuthenticator::builder(secret)
            .build()
            .await
            .expect("failed to create authenticator");
        Client {
            inner_client: Arc::new(InnerClient {
                authenticator,
                reqwest_client: reqwest::Client::new(),
            }),
        }
    }
    pub async fn post_query(&self, project_id: &str, query: String) -> Result<Job, BigQueryError> {
        let api_url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs",
            project_id = project_id
        );
        let tok = self.inner_client.authenticator.token(SCOPES).await?;
        let job = structs::job::Job::new(query);
        let res = self
            .inner_client
            .reqwest_client
            .post(api_url)
            .json(&job)
            .bearer_auth(tok.as_str())
            //.bearer_auth("ya29.A0AVA9y1tiD-iC_4ZtxKTy2bj6SHkSsvcebvjS9R0H0cTDeKmS5aId1vw9p5eKm4u3CYCDqk901sBC4PgCs6Ba1bHU63HgpBXBsderFEQbUySmNGpZdOaYLdkYLdzIhf-wE546N2UF0O9-wWhww2nFrPxEnKuWYUNnWUtBVEFTQVRBU0ZRRTY1ZHI4dGZ3U1FKaDMyajNfSm1BX0ltcG9KUQ0163")
            .send()
            .await?;

        Ok(Job {
            inner_job: res.json().await?,
            inner_client: self.inner_client.clone(),
            project_id: project_id.into(),
        })
    }
}

#[derive(Clone)]
pub struct Job {
    inner_client: Arc<InnerClient>,
    inner_job: structs::job::Job,
    project_id: String,
}

impl fmt::Debug for Job {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Job")
            .field("inner_job", &self.inner_job)
            .field("project_id", &self.project_id)
            .finish()
    }
}

impl Job {
    pub async fn get_results(&self) -> Result<structs::query_results::QueryResults, BigQueryError> {
        if let Some(job_id) = self
            .inner_job
            .job_reference
            .as_ref()
            .and_then(|job| job.job_id.as_ref())
        {
            let api_url = format!(
                "https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries/{job_id}",
                project_id = self.project_id,
                job_id = job_id,
            );
            let tok = self.inner_client.authenticator.token(SCOPES).await?;
            let res = self
                .inner_client
                .reqwest_client
                .get(api_url)
                .bearer_auth(tok.as_str())
                .send()
                .await?;
            //println!("Resp body: {}", res.text().await.unwrap());
            let res = res.json().await?;
            Ok(res)
        } else {
            Err(BigQueryError::MissingJobIdInGoogleApiResponse)
        }
    }
}
