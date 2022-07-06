use firestore_grpc::tonic::{
    codegen::InterceptedService,
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig},
    Request, Status,
};
use firestore_grpc::v1::{firestore_client::FirestoreClient, BatchGetDocumentsRequest};

use std::time::Instant;

use std::env;

const URL: &'static str = "https://firestore.googleapis.com";
const DOMAIN: &'static str = "firestore.googleapis.com";

pub type BoxError = Box<dyn std::error::Error + Sync + Send + 'static>;

fn get_token() -> String {
    env::var("TOKEN").unwrap()
}
async fn get_client() -> Result<
    FirestoreClient<
        InterceptedService<Channel, impl Fn(Request<()>) -> Result<Request<()>, Status>>,
    >,
    BoxError,
> {
    let endpoint = Channel::from_static(URL).tls_config(ClientTlsConfig::new().domain_name(DOMAIN));

    let bearer_token = format!("Bearer {}", get_token());
    let header_value = MetadataValue::from_str(&bearer_token).unwrap();

    let channel = endpoint.unwrap().connect().await.unwrap();

    let service = FirestoreClient::with_interceptor(channel, move |mut req: Request<()>| {
        req.metadata_mut()
            .insert("authorization", header_value.clone());
        Ok(req)
    });
    Ok(service)
}

async fn get_documents() -> Result<(), BoxError> {
    let parent: String = "projects/topliner-c3bc2/databases/(default)".into();
    let mut client = get_client().await.unwrap();
    let documents = vec![
        "NUhMKcQTVW1mqv9BuH6c",
        "GsTt18RhnD3Plow3vyD4",
        "CjqMI1rDi2YCi3V9SFbS",
        "0lT7rce9WbwYayOy4nMY",
        "8hDgybm5jGlD7Zf02U1C",
        "HeKgbXOjGar6tVVlnzAR",
        "iTJd1NQr9P917SgON8Mh",
        "uLuIPgOoCxpn6Mz7eSix",
        "lLR3bSDqNQs6XzgCya7d",
        "XMSSxsGLFdA7BOLqbPYC",
        "kPIpO9D1w9H9TSK5FLyv",
        "W4vIAbxwLZSRSRgmg1mt",
        "4dtbVdo3jSZXglGKs0Hy",
        "iVqfAVA05poef6rE2Uk0",
        "8zta4ExMaEw7zbw6tlXW",
        "0WD1MZOZ2Bp6X3vSkIQP",
        "1wZ29jgcfm46MUMpGV88",
        "LSjc28yu4KDRJ9s94oQ3",
        "FwHrKevwxGMqNabi1o0d",
        "6BQeYUxpCaGaCXo1Bx5P",
        "x9PaUKDUk4SG55abnHVV",
        "UOHK9yvxJzxF2Nom9ayO",
        "sJXZ5AcDaAf6QG3J656F",
        "JTwX3N1Q2hvjRH2E2wyv",
        "rHkNLHQOeoaXbLdtyczs",
    ]
    .iter()
    .map(|s| {
        format!(
            "projects/topliner-c3bc2/databases/(default)/documents/voises/{}",
            *s
        )
    })
    .collect();
    let start = Instant::now();
    let mut stream = client
        .batch_get_documents(BatchGetDocumentsRequest {
            database: parent,
            documents,
            mask: None,
            consistency_selector: None,
        })
        .await
        .unwrap()
        .into_inner();
    while let Ok(Some(item)) = stream.message().await {
        println!("\treceived: {:?}", item);
    }
    let duration = start.elapsed();

    println!("Time elapsed in expensive_function() is: {:?}", duration);

    //while let Some(doc) = res.next().await {}
    return Ok(());
}

#[tokio::main]
async fn main() {
    let res = get_documents().await.unwrap();
    println!("{:?}", res);
}
