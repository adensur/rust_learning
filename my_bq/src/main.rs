mod client;
mod error;
mod structs;

const PROJECT_ID: &str = "voisey-feed-ranking";
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scopes = &["https://www.googleapis.com/auth/bigquery"];

    let secret = yup_oauth2::read_authorized_user_secret(
        "/Users/mgaiduk/.config/gcloud/application_default_credentials.json",
    )
    .await
    .unwrap();
    let authenticator = yup_oauth2::AuthorizedUserAuthenticator::builder(secret)
        .build()
        .await
        .expect("failed to create authenticator");
    let tok = authenticator.token(scopes).await.unwrap();
    println!("token is: {:?}, str: {}", tok, tok.as_str());

    let client = client::Client::new().await;
    let job = client.post_query(PROJECT_ID, "SELECT * FROM `bigquery-public-data.covid19_public_forecasts.county_14d_historical_` LIMIT 4;".into()).await?;
    println!("Created job: {:?}", job);

    /*if false {
        let api_url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries/{job_id}",
            project_id = PROJECT_ID,
            job_id = "job_APWNQYieC7dZJnVdmiOJ7ySpEYqe"
        );
        let res = client
            .get(api_url)
            .bearer_auth(tok.as_str())
            //.bearer_auth("ya29.A0AVA9y1tiD-iC_4ZtxKTy2bj6SHkSsvcebvjS9R0H0cTDeKmS5aId1vw9p5eKm4u3CYCDqk901sBC4PgCs6Ba1bHU63HgpBXBsderFEQbUySmNGpZdOaYLdkYLdzIhf-wE546N2UF0O9-wWhww2nFrPxEnKuWYUNnWUtBVEFTQVRBU0ZRRTY1ZHI4dGZ3U1FKaDMyajNfSm1BX0ltcG9KUQ0163")
            .send()
            .await?;
        println!("{:?}", res);
        println!("Resp body: {}", res.text().await.unwrap());
    }*/

    Ok(())
}
