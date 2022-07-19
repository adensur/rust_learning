use crate::error::BigQueryError;
use yup_oauth2::authenticator::DefaultAuthenticator;
pub struct Client {
    authenticator: DefaultAuthenticator,
    reqwest_client: reqwest::Client,
}

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
            authenticator,
            reqwest_client: reqwest::Client::new(),
        }
    }
    pub async fn post_query(&self, project_id: &str) -> Result<(), BigQueryError> {
        let api_url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs",
            project_id = project_id
        );
        let scopes = &["https://www.googleapis.com/auth/bigquery"];
        let tok = self.authenticator.token(scopes).await?;
        let res = self.reqwest_client
          .post(api_url)
          .body(r#"{
              "configuration": {
                  "query": {
                      "query": "SELECT * FROM `bigquery-public-data.covid19_public_forecasts.county_14d_historical_` LIMIT 10",
                      "useLegacySql": false
                  }
                }
            }"#)
          .bearer_auth(tok.as_str())
          //.bearer_auth("ya29.A0AVA9y1tiD-iC_4ZtxKTy2bj6SHkSsvcebvjS9R0H0cTDeKmS5aId1vw9p5eKm4u3CYCDqk901sBC4PgCs6Ba1bHU63HgpBXBsderFEQbUySmNGpZdOaYLdkYLdzIhf-wE546N2UF0O9-wWhww2nFrPxEnKuWYUNnWUtBVEFTQVRBU0ZRRTY1ZHI4dGZ3U1FKaDMyajNfSm1BX0ltcG9KUQ0163")
          .send()
          .await.unwrap();
        println!("{:?}", res);
        println!("Resp body: {}", res.text().await.unwrap());
        Ok(())
    }
}
