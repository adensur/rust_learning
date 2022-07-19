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
    job.get_results().await.unwrap();

    Ok(())
}
