mod client;
mod error;
mod structs;

use anyhow::Result;
use client::{Decoder, Deserialize};
use error::BigQueryError;
use my_bq_proc::Deserialize;
use structs::{
    row_field::Value,
    table_field_schema::{self, TableFieldSchema},
    table_row::TableRow,
};

#[derive(Debug, Deserialize)]
struct PrivacyInfo {
    analytics_storage: String,
    ads_storage: String,
    uses_transient_token: String,
}

#[derive(Debug, Deserialize)]
struct JsonValue {
    string_value: Option<String>,
    int_value: Option<i64>,
    float_value: Option<f64>,
    double_value: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct EventParam {
    key: String,
    value: JsonValue,
}

#[derive(Debug, Deserialize)]
struct Struct3 {
    user_id: String,
    user_id_nullable: Option<String>,
    event_timestamp: i64,
    privacy_info: PrivacyInfo,
    event_params: Vec<EventParam>,
    user_properties: Vec<EventParam>,
}

const PROJECT_ID: &str = "voisey-feed-ranking";
#[tokio::main]
async fn main() -> Result<()> {
    let client = client::Client::new().await;
    let job = client.post_query(PROJECT_ID, r#"select 
        coalesce(user_id, "") as user_id,
        user_id as user_id_nullable,
        event_timestamp,
        privacy_info,
        event_params,
        user_properties,
        from `topliner-c3bc2.analytics_161560246.events_*`
        where event_name in 
            ('voisey_entered', 'voisey_exited', 'liked_voisey', 'unliked_voisey', 'shared_voisey_to_third_party', 'posted_comment_on_voisey',
            'record_tapped' ) 
            and app_info.version >= "1.61" 
            and _TABLE_SUFFIX between "20220401" and "20220515" limit 100000;"#.into()).await?;
    println!("Created job: {:?}", job);
    let results = job.get_results::<Struct3>().await.unwrap();
    println!("Results len: {}", results.len());
    for row in results {
        //println!("Got record: {:?}", row);
    }
    Ok(())
}
