mod client;
mod error;
mod structs;

use anyhow::Result;
use client::{Decoder, Deserialize};
use error::BigQueryError;
use structs::{
    row_field::Value,
    table_field_schema::{self, TableFieldSchema},
    table_row::TableRow,
};

/*
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[my_bq(rename_all = "camelCase")]
pub struct TableRow {
    #[my_bq(rename = "f", skip_serializing_if = "Option::is_none")]
    pub state_name: Option<String>,
}
*/
/*
#[derive(Debug)]
struct JsonValue {
    string_value: Option<String>,
    int_value: Option<i64>,
    float_value: Option<f32>,
    double_value: Option<f64>,
}

fn get_opt_string_value(
    row: &TableRow,
    idx: usize,
    key: &str,
) -> Result<Option<String>, BigQueryError> {
    let string_value_field =
        row.values
            .get(idx)
            .ok_or(BigQueryError::RowSchemaMismatch(format!(
                "Expected at least {} fields, have only {} for key: {}",
                idx,
                row.values.len(),
                key
            )))?;
    match string_value_field.value {
        Some(serde_json::Value::String(val)) => Ok(Some(val)),
        Some(serde_json::Value::Null) => Ok(None),
        _ => {
            return Err(BigQueryError::RowSchemaMismatch(
                "todo: proper error msg!".into(),
            ))
        }
    }
}

fn get_string_value(row: &TableRow, idx: usize, key: &str) -> Result<String, BigQueryError> {
    let string_value_field =
        row.values
            .get(idx)
            .ok_or(BigQueryError::RowSchemaMismatch(format!(
                "Expected at least {} fields, have only {} for key: {}",
                idx,
                row.values.len(),
                key
            )))?;
    match string_value_field.value {
        Some(serde_json::Value::String(val)) => Ok(val),
        _ => {
            return Err(BigQueryError::RowSchemaMismatch(
                "todo: proper error msg!".into(),
            ))
        }
    }
}

impl Deserialize for JsonValue {
    fn CreateDeserializeIndices(
        schema_fields: Vec<TableFieldSchema>,
    ) -> Result<Decoder, BigQueryError> {
        let mut indices = Vec::with_capacity(1);
        let mut recursive_indices = Vec::with_capacity(1);
        for (i, field) in schema_fields.iter().enumerate() {
            match &field.name {
                Some(name) if name == "string_value" => {
                    if field.field_type != Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type String for field 'string_value', found {:?}",
                            field.field_type
                        )));
                    }
                    indices.push(i);
                }
                Some(name) if name == "int_value" => {
                    if field.field_type != Type::Integer {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type String for field 'int_value', found {:?}",
                            field.field_type
                        )));
                    }
                    indices.push(i);
                }
                Some(name) if name == "float_value" => {
                    if field.field_type != Type::Float {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type String for field 'float_value', found {:?}",
                            field.field_type
                        )));
                    }
                    indices.push(i);
                }
                Some(name) if name == "double_value" => {
                    if field.field_type != Type::Float {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type String for field 'double_value', found {:?}",
                            field.field_type
                        )));
                    }
                    indices.push(i);
                }
                _ => continue,
            }
        }
        return Ok(Decoder {
            indices,
            recursive_indices,
        });
    }
    fn Deserialize(mut row: &mut TableRow, decoder: &Decoder) -> Result<JsonValue, BigQueryError> {
        let string_value = get_opt_string_value(&row, decoder.indices[0], "string_value")?;
        Ok(JsonValue {
            string_value,
            int_value: None,
            float_value: None,
            double_value: None,
        })
    }
}

#[derive(Debug)]
struct EventParam {
    key: String,
    value: JsonValue,
}

#[derive(Debug)]
pub struct Record {
    pub event_name: String,
    pub event_params: Vec<EventParam>,
}

impl Deserialize for EventParam {
    fn CreateDeserializeIndices(
        schema_fields: Vec<TableFieldSchema>,
    ) -> Result<Decoder, BigQueryError> {
        let mut indices = Vec::with_capacity(1);
        let mut recursive_indices = Vec::with_capacity(1);
        for (i, field) in schema_fields.iter().enumerate() {
            match &field.name {
                Some(name) if name == "key" => {
                    if field.field_type != Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type String for field 'key', found {:?}",
                            field.field_type
                        )));
                    }
                    indices.push(i);
                }
                Some(name) if name == "value" => {
                    if field.field_type != Type::Record {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type Record for field 'value', found {:?}",
                            field.field_type
                        )));
                    }
                    if let Some(subfields) = field.fields {
                        indices.push(i);
                        recursive_indices
                            .push(Box::new(JsonValue::CreateDeserializeIndices(subfields)?));
                    } else {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type Record for field 'event_params', found {:?}",
                            field.field_type
                        )));
                    }
                }
                _ => continue,
            }
        }
        return Ok(Decoder {
            indices,
            recursive_indices,
        });
    }
    fn Deserialize(mut row: &mut TableRow, decoder: &Decoder) -> Result<EventParam, BigQueryError> {
        let key = get_string_value(&row, decoder.indices[0], "string_value")?;
        let value = JsonValue::Deserialize(&mut row, &decoder.recursive_indices[0])?;
        Ok(EventParam { key, value })
    }
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
    fn CreateDeserializeIndices(
        schema_fields: Vec<TableFieldSchema>,
    ) -> Result<Decoder, BigQueryError> {
        let mut indices = Vec::with_capacity(1);
        let mut recursive_indices = Vec::with_capacity(1);
        for (i, field) in schema_fields.iter().enumerate() {
            match &field.name {
                Some(name) if name == "event_name" => {
                    if field.field_type != Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type String for field 'event_name', found {:?}",
                            field.field_type
                        )));
                    }
                    indices.push(i);
                }
                Some(name) if name == "event_params" => {
                    if field.field_type != Type::Record {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type Record for field 'event_params', found {:?}",
                            field.field_type
                        )));
                    }
                    if let Some(subfields) = field.fields {
                        recursive_indices
                            .push(Box::new(EventParam::CreateDeserializeIndices(subfields)?));
                    } else {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected type Record for field 'event_params', found {:?}",
                            field.field_type
                        )));
                    }
                }
                _ => continue,
            }
        }
        return Ok(Decoder {
            indices,
            recursive_indices,
        });
    }

    fn Deserialize(row: &mut TableRow, decoder: &Decoder) -> Result<Record, BigQueryError> {
        let event_name = get_string_value(row, decoder.indices[0], "event_name")?;
        let event_params = EventParam::Deserialize(row, &decoder.recursive_indices[0])?;
        Ok(Record {
            event_name,
            event_params,
        })
    }
}
*/
#[derive(Debug)]
struct Struct1 {
    user_id: String,
}

impl Deserialize for Struct1 {
    fn create_deserialize_indices(
        schema_fields: Vec<TableFieldSchema>,
    ) -> Result<Decoder, BigQueryError> {
        let mut indices: Vec<usize> = vec![usize::MAX; 1];
        for (i, field) in schema_fields.iter().enumerate() {
            if field.name == "user_id" {
                if field.field_type != table_field_schema::Type::String {
                    return Err(BigQueryError::RowSchemaMismatch(format!(
                        "Expected String type for field user_id, got {:?}",
                        field.field_type
                    )));
                }
                indices[0] = i;
            }
        }
        // check that all indices are filled
        if indices[0] == usize::MAX {
            return Err(BigQueryError::RowSchemaMismatch(
                "Failed to find field 'user_id' in schema".to_string(),
            ));
        }
        Ok(Decoder {
            indices,
            recursive_indices: Vec::new(),
        })
    }
    fn deserialize(mut row: TableRow, decoder: &Decoder) -> Result<Self, BigQueryError> {
        let user_id_idx = decoder.indices[0];
        if row.fields.len() <= user_id_idx {
            return Err(BigQueryError::NotEnoughFields {
                expected: user_id_idx + 1,
                found: row.fields.len(),
            });
        }
        let user_id = std::mem::take(&mut row.fields[user_id_idx]);
        let user_id = match user_id.value {
            Value::String(val) => val,
            other_value => {
                return Err(BigQueryError::UnexpectedFieldType(format!(
                    "Expected integer value, found {:?}",
                    other_value
                )))
            }
        };
        Ok(Self { user_id })
    }
}

const PROJECT_ID: &str = "voisey-feed-ranking";
#[tokio::main]
async fn main() -> Result<()> {
    let client = client::Client::new().await;
    let job = client.post_query(PROJECT_ID, r#"select 
        coalesce(user_id, "") as user_id,
        event_timestamp
        from `topliner-c3bc2.analytics_161560246.events_*`
        where event_name in 
            ('voisey_entered', 'voisey_exited', 'liked_voisey', 'unliked_voisey', 'shared_voisey_to_third_party', 'posted_comment_on_voisey',
            'record_tapped' ) 
            and app_info.version >= "1.61" 
            and _TABLE_SUFFIX between "20220401" and "20220402" limit 1;"#.into()).await?;
    println!("Created job: {:?}", job);
    let results = job.get_results::<Struct1>().await.unwrap();
    for row in results {
        println!("Got record: {:?}", row);
    }
    Ok(())
}
