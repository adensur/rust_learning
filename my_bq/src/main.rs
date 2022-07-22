mod client;
mod error;
mod structs;
use crate::structs::table_field_schema::Type;
use crate::structs::table_schema::TableSchema;

use client::Deserialize;
use error::BigQueryError;
use structs::{row_field::RowField, table_row::TableRow};

/*
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[my_bq(rename_all = "camelCase")]
pub struct TableRow {
    #[my_bq(rename = "f", skip_serializing_if = "Option::is_none")]
    pub state_name: Option<String>,
}
*/

#[derive(Debug)]
pub struct Record {
    pub state_name: String,
}

impl Deserialize for Record {
    fn CreateDeserializeIndices(schema: TableSchema) -> Result<Vec<usize>, BigQueryError> {
        let mut res = Vec::with_capacity(1);
        for (i, field) in schema.fields.iter().enumerate() {
            match &field.name {
                Some(name) if name == "state_name" => {
                    if field.field_type != Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type String for field 'state_name', found {:?}",
                            field.field_type
                        )));
                    }
                    res.push(i);
                }
                _ => continue,
            }
        }
        return Ok(res);
    }

    fn Deserialize(mut row: TableRow, indices: &[usize]) -> Result<Record, BigQueryError> {
        if let Some(values) = &mut row.values {
            if let Some(RowField {
                value: Some(serde_json::Value::String(state_name)),
            }) = values.get(indices[0]).take()
            {
                Ok(Record {
                    state_name: state_name.to_string(), // todo: avoid extra copy here
                })
            } else {
                Err(BigQueryError::RowSchemaMismatch("No value found".into()))
            }
        } else {
            Err(BigQueryError::MissingRowsInQueryResponse)
        }
    }
}

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
    let results = job.get_results::<Record>().await.unwrap();
    for row in results {
        println!("Got record: {:?}", row);
    }
    Ok(())
}
