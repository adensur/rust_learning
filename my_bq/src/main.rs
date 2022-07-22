mod client;
mod error;
mod structs;
use crate::structs::table_field_schema::Type;
use crate::structs::table_schema::TableSchema;

use anyhow::{Context, Result};
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
    pub event_name: String,
}

/*
type RawEvent struct {
    EventName                  string `bigquery:"event_name"`
    UserId                     string `bigquery:"user_id"`
    UserPseudoId               string `bigquery:"user_pseudo_id"`
    EventTimestampMicroseconds int64  `bigquery:"event_timestamp_microseconds"`
    EventParams                string `bigquery:"event_params"`
    UserProperties             string `bigquery:"user_properties"`
    Version                    string `bigquery:"version"`
}*/

impl Deserialize for Record {
    fn CreateDeserializeIndices(schema: TableSchema) -> Result<Vec<usize>, BigQueryError> {
        let mut res = Vec::with_capacity(1);
        for (i, field) in schema.fields.iter().enumerate() {
            match &field.name {
                Some(name) if name == "event_name" => {
                    if field.field_type != Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type String for field 'event_name', found {:?}",
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
                value: Some(serde_json::Value::String(val)),
            }) = values.get(indices[0]).take()
            {
                Ok(Record {
                    event_name: val.to_string(), // todo: avoid extra copy here
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
async fn main() -> Result<()> {
    let client = client::Client::new().await;
    let job = client.post_query(PROJECT_ID, r#"select 
        coalesce(user_id, "") as user_id,
        coalesce(user_pseudo_id, "") as user_pseudo_id, 
        coalesce(app_info.version, "") as version,
        event_timestamp as event_timestamp_microseconds, 
        event_name,
        to_json_string(event_params) as event_params,
        to_json_string(user_properties) as user_properties,
        from `topliner-c3bc2.analytics_161560246.events_*`
        where event_name in 
            ('voisey_entered', 'voisey_exited', 'liked_voisey', 'unliked_voisey', 'shared_voisey_to_third_party', 'posted_comment_on_voisey',
            'record_tapped' ) 
            and app_info.version >= "1.61" 
            and _TABLE_SUFFIX between "20220401" and "20220402" limit 10;"#.into()).await?;
    println!("Created job: {:?}", job);
    let results = job.get_results::<Record>().await.unwrap();
    for row in results {
        println!("Got record: {:?}", row);
    }
    Ok(())
}
