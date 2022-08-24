use std::fmt;
use std::sync::Arc;

use crate::error::BigQueryError;
use crate::structs;
use crate::structs::error_proto::ErrorProto;
use crate::structs::job_status::JobStatus;
use crate::structs::query_results::QueryResults;
use crate::structs::table_field_schema::TableFieldSchema;
use crate::TableRow;
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
            .send()
            .await?;
        let job: structs::job::Job = res.json().await?;
        if let Some(JobStatus {
            error_result: Some(ErrorProto { message, .. }),
            ..
        }) = job.status
        {
            return Err(BigQueryError::JobInsertError { msg: message });
        } else {
            if let Some(JobStatus {
                errors: Some(errors),
                ..
            }) = &job.status
            {
                for error in errors {
                    println!("Got error in job insert request: {}", error.message);
                }
            }
            Ok(Job {
                inner_job: job,
                inner_client: self.inner_client.clone(),
                project_id: project_id.into(),
            })
        }
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

#[derive(Default)]
pub struct Decoder {
    pub indices: Vec<usize>,
    pub recursive_indices: Vec<Box<Decoder>>,
}

pub trait Deserialize
where
    Self: Sized,
{
    fn create_deserialize_indices(
        schema_fields: &Vec<TableFieldSchema>,
    ) -> Result<Decoder, BigQueryError>;
    fn deserialize(row: TableRow, decoder: &Decoder) -> Result<Self, BigQueryError>;
}

impl Job {
    pub async fn get_results<T: Deserialize>(&self) -> Result<Vec<T>, BigQueryError> {
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
            println!("Resp body: {}", res.text().await.unwrap());
            panic!("");
            let query_results: QueryResults = res.json().await?;
            println!("query results: {:?}", query_results);
            let indices = T::create_deserialize_indices(&query_results.schema.fields)?;
            let res: Result<Vec<T>, BigQueryError> = query_results
                .rows
                .into_iter()
                .map(|row| T::deserialize(row, &indices))
                .collect();
            Ok(res?)
        } else {
            Err(BigQueryError::MissingJobIdInGoogleApiResponse)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::structs::row_field::Value;
    use crate::structs::{table_field_schema, table_row::TableRow, table_schema::TableSchema};
    use crate::BigQueryError;

    use super::*;

    use my_bq_proc::Deserialize;

    #[derive(Deserialize)]
    struct MyStruct2 {
        #[my_bq(rename = "ads_storage2")]
        ads_storage: String,
    }

    #[test]
    fn test_simplest_struct() {
        let schema = r#"{
            "fields": [
                {
                "name": "ads_storage2",
                "type": "STRING",
                "mode": "NULLABLE"
                }
            ]
          }"#;
        let schema: TableSchema = serde_json::from_str(schema).unwrap();
        assert_eq!(schema.fields.len(), 1);
        let row = r#"{"f": [
            {
              "v": "Yes"
            }
          ]
        }"#;
        let row: TableRow = serde_json::from_str(row).unwrap();
        assert_eq!(row.fields.len(), 1);
        let decoder = MyStruct2::create_deserialize_indices(&schema.fields).unwrap();
        assert_eq!(decoder.indices.len(), 1);
        let rec = MyStruct2::deserialize(row, &decoder).unwrap();
        assert_eq!(rec.ads_storage, "Yes");
    }

    struct PrivacyInfo {
        analytics_storage: String,
        ads_storage: String,
        uses_transient_token: String,
    }

    impl Deserialize for PrivacyInfo {
        fn create_deserialize_indices(
            schema_fields: &Vec<TableFieldSchema>,
        ) -> Result<Decoder, BigQueryError> {
            let mut indices: Vec<usize> = vec![usize::MAX; 3];
            for (i, field) in schema_fields.iter().enumerate() {
                if field.name == "analytics_storage" {
                    if field.field_type != table_field_schema::Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected String type for field analytics_storage, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[0] = i;
                } else if field.name == "ads_storage" {
                    if field.field_type != table_field_schema::Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected String type for field ads_storage, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[1] = i;
                }
                if field.name == "uses_transient_token" {
                    if field.field_type != table_field_schema::Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected String type for field uses_transient_token, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[2] = i;
                }
            }
            // check that all indices are filled
            if indices[0] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'analytics_storage' in schema".to_string(),
                ));
            }
            if indices[1] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'ads_storage' in schema".to_string(),
                ));
            }
            if indices[2] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'uses_transient_token' in schema".to_string(),
                ));
            }
            Ok(Decoder {
                indices,
                recursive_indices: Vec::new(),
            })
        }
        fn deserialize(mut row: TableRow, decoder: &Decoder) -> Result<Self, BigQueryError> {
            let analytics_storage_idx = decoder.indices[0];
            if row.fields.len() <= analytics_storage_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: analytics_storage_idx + 1,
                    found: row.fields.len(),
                });
            }
            let analytics_storage = std::mem::take(&mut row.fields[analytics_storage_idx]);
            let analytics_storage = match analytics_storage.value {
                Some(Value::String(val)) => val,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field analytics_storage, found {:?}",
                        other_value
                    )))
                }
                None => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected required value for field analytics_storage, found null",
                    )))
                }
            };

            let ads_storage_idx = decoder.indices[1];
            if row.fields.len() <= ads_storage_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: ads_storage_idx + 1,
                    found: row.fields.len(),
                });
            }
            let ads_storage = std::mem::take(&mut row.fields[ads_storage_idx]);
            let ads_storage = match ads_storage.value {
                Some(Value::String(val)) => val,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field ads_storage, found {:?}",
                        other_value
                    )))
                }
                None => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected required value for field ads_storage, found null",
                    )))
                }
            };

            let uses_transient_token_idx = decoder.indices[2];
            if row.fields.len() <= uses_transient_token_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: uses_transient_token_idx + 1,
                    found: row.fields.len(),
                });
            }
            let uses_transient_token = std::mem::take(&mut row.fields[uses_transient_token_idx]);
            let uses_transient_token = match uses_transient_token.value {
                Some(Value::String(val)) => val,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field uses_transient_token, found {:?}",
                        other_value
                    )))
                }
                None => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected required value for field uses_transient_token, found null",
                    )))
                }
            };

            Ok(Self {
                analytics_storage,
                ads_storage,
                uses_transient_token,
            })
        }
    }
    #[test]
    fn it_works() {
        let schema = r#"{
            "fields": [
                {
                "name": "analytics_storage",
                "type": "STRING",
                "mode": "NULLABLE"
                },
                {
                "name": "ads_storage",
                "type": "STRING",
                "mode": "NULLABLE"
                },
                {
                "name": "uses_transient_token",
                "type": "STRING",
                "mode": "NULLABLE"
                }
            ]
          }"#;
        let schema: TableSchema = serde_json::from_str(schema).unwrap();
        assert_eq!(schema.fields.len(), 3);
        let row = r#"{"f": [
            {
              "v": "Yes"
            },
            {
              "v": "Yes"
            },
            {
              "v": "No"
            }
          ]
        }"#;
        let row: TableRow = serde_json::from_str(row).unwrap();
        assert_eq!(row.fields.len(), 3);
        let decoder = PrivacyInfo::create_deserialize_indices(&schema.fields).unwrap();
        assert_eq!(decoder.indices.len(), 3);
        let rec = PrivacyInfo::deserialize(row, &decoder).unwrap();
        assert_eq!(rec.analytics_storage, "Yes");
        assert_eq!(rec.ads_storage, "Yes");
        assert_eq!(rec.uses_transient_token, "No");
    }

    struct JsonValue {
        string_value: Option<String>,
        int_value: Option<i64>,
        float_value: Option<f64>,
        double_value: Option<f64>,
    }

    impl Deserialize for JsonValue {
        fn create_deserialize_indices(
            schema_fields: &Vec<TableFieldSchema>,
        ) -> Result<Decoder, BigQueryError> {
            let mut indices: Vec<usize> = vec![usize::MAX; 4];
            for (i, field) in schema_fields.iter().enumerate() {
                if field.name == "string_value" {
                    if field.field_type != table_field_schema::Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected String type for field string_value, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[0] = i;
                } else if field.name == "int_value" {
                    if field.field_type != table_field_schema::Type::Integer {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected Integer type for field int_value, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[1] = i;
                } else if field.name == "float_value" {
                    if field.field_type != table_field_schema::Type::Float {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected Float type for field float_value, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[2] = i;
                } else if field.name == "double_value" {
                    if field.field_type != table_field_schema::Type::Float {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected Float type for field double_value, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[3] = i;
                }
            }
            // check that all indices are filled
            if indices[0] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'string_value' in schema".to_string(),
                ));
            }
            if indices[1] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'int_value' in schema".to_string(),
                ));
            }
            if indices[2] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'float_value' in schema".to_string(),
                ));
            }
            if indices[3] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'double_value' in schema".to_string(),
                ));
            }
            Ok(Decoder {
                indices,
                recursive_indices: Vec::new(),
            })
        }
        fn deserialize(mut row: TableRow, decoder: &Decoder) -> Result<Self, BigQueryError> {
            let string_value_idx = decoder.indices[0];
            if row.fields.len() <= string_value_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: string_value_idx + 1,
                    found: row.fields.len(),
                });
            }
            let string_value = std::mem::take(&mut row.fields[string_value_idx]);
            let string_value = match string_value.value {
                Some(Value::String(val)) => Some(val),
                None => None,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field string_value, found {:?}",
                        other_value
                    )))
                }
            };

            let int_value_idx = decoder.indices[1];
            if row.fields.len() <= int_value_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: int_value_idx + 1,
                    found: row.fields.len(),
                });
            }
            let int_value = std::mem::take(&mut row.fields[int_value_idx]);
            let int_value = match int_value.value {
                Some(Value::String(val)) => Some(val.parse()?),
                None => None,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field int_value, found {:?}",
                        other_value
                    )))
                }
            };

            let float_value_idx = decoder.indices[2];
            if row.fields.len() <= float_value_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: float_value_idx + 1,
                    found: row.fields.len(),
                });
            }
            let float_value = std::mem::take(&mut row.fields[float_value_idx]);
            let float_value = match float_value.value {
                Some(Value::String(val)) => Some(val.parse()?),
                None => None,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field float_value, found {:?}",
                        other_value
                    )))
                }
            };

            let double_value_idx = decoder.indices[3];
            if row.fields.len() <= double_value_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: double_value_idx + 1,
                    found: row.fields.len(),
                });
            }
            let double_value = std::mem::take(&mut row.fields[double_value_idx]);
            let double_value = match double_value.value {
                Some(Value::String(val)) => Some(val.parse()?),
                None => None,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field double_value, found {:?}",
                        other_value
                    )))
                }
            };

            Ok(Self {
                string_value,
                int_value,
                float_value,
                double_value,
            })
        }
    }
    #[test]
    fn test_json_value() {
        let schema = r#"{
            "fields": [
              {
                "name": "string_value",
                "type": "STRING",
                "mode": "NULLABLE"
              },
              {
                "name": "int_value",
                "type": "INTEGER",
                "mode": "NULLABLE"
              },
              {
                "name": "float_value",
                "type": "FLOAT",
                "mode": "NULLABLE"
              },
              {
                "name": "double_value",
                "type": "FLOAT",
                "mode": "NULLABLE"
              }
            ]
          }"#;
        let schema: TableSchema = serde_json::from_str(schema).unwrap();
        assert_eq!(schema.fields.len(), 4);
        let row = r#"{"f": [
                  {
                    "v": null
                  },
                  {
                    "v": null
                  },
                  {
                    "v": null
                  },
                  {
                    "v": "0.0"
                  }
                ]
              }"#;
        let row: TableRow = serde_json::from_str(row).unwrap();
        assert_eq!(row.fields.len(), 4);
        let decoder = JsonValue::create_deserialize_indices(&schema.fields).unwrap();
        assert_eq!(decoder.indices.len(), 4);
        let rec = JsonValue::deserialize(row, &decoder).unwrap();
        assert!(rec.string_value.is_none());
        assert!(rec.int_value.is_none());
        assert!(rec.float_value.is_none());
        assert_eq!(rec.double_value, Some(0.0));
    }

    struct EventParam {
        key: String,
        value: JsonValue,
    }

    impl Deserialize for EventParam {
        fn create_deserialize_indices(
            schema_fields: &Vec<TableFieldSchema>,
        ) -> Result<Decoder, BigQueryError> {
            let mut indices: Vec<usize> = vec![usize::MAX; 2];
            let mut recursive_indices: Vec<Box<Decoder>> = Vec::new();
            for i in 0..1 {
                recursive_indices.push(Box::new(Decoder::default()));
            }
            for (i, field) in schema_fields.iter().enumerate() {
                if field.name == "key" {
                    if field.field_type != table_field_schema::Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected String type for field key, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[0] = i;
                } else if field.name == "value" {
                    if field.field_type != table_field_schema::Type::Record {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected Record type for field value, got {:?}",
                            field.field_type
                        )));
                    }
                    match &field.fields {
                        Some(fields) => {
                            let decoder = JsonValue::create_deserialize_indices(&fields)?;
                            indices[1] = i;
                            recursive_indices[0] = Box::new(decoder);
                        }
                        None => {
                            return Err(BigQueryError::RowSchemaMismatch(format!(
                                "Failed to find recursive schema for field value",
                            )))
                        }
                    }
                }
            }
            // check that all indices are filled
            if indices[0] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'key' in schema".to_string(),
                ));
            }
            if indices[1] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'value' in schema".to_string(),
                ));
            }
            Ok(Decoder {
                indices,
                recursive_indices,
            })
        }
        fn deserialize(mut row: TableRow, decoder: &Decoder) -> Result<Self, BigQueryError> {
            let key_idx = decoder.indices[0];
            if row.fields.len() <= key_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: key_idx + 1,
                    found: row.fields.len(),
                });
            }
            let key = std::mem::take(&mut row.fields[key_idx]);
            let key = match key.value {
                Some(Value::String(val)) => val,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field key, found {:?}",
                        other_value
                    )))
                }
                None => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected required value for field key, found null",
                    )))
                }
            };

            let value_idx = decoder.indices[1];
            if row.fields.len() <= value_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: value_idx + 1,
                    found: row.fields.len(),
                });
            }
            let value = std::mem::take(&mut row.fields[value_idx]);
            let value = match value.value {
                Some(Value::Record(val)) => {
                    JsonValue::deserialize(val, &decoder.recursive_indices[0])?
                }
                None => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected required value for field value, found null",
                    )))
                }
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field user_id_nullable, found {:?}",
                        other_value
                    )))
                }
            };

            Ok(Self { key, value })
        }
    }
    #[test]
    fn test_event_param() {
        let schema = r#"{
            "fields": [
          {
            "name": "key",
            "type": "STRING",
            "mode": "NULLABLE"
          },
          {
            "name": "value",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
              {
                "name": "string_value",
                "type": "STRING",
                "mode": "NULLABLE"
              },
              {
                "name": "int_value",
                "type": "INTEGER",
                "mode": "NULLABLE"
              },
              {
                "name": "float_value",
                "type": "FLOAT",
                "mode": "NULLABLE"
              },
              {
                "name": "double_value",
                "type": "FLOAT",
                "mode": "NULLABLE"
              }
            ]
          }
        ]
      }"#;
        let schema: TableSchema = serde_json::from_str(schema).unwrap();
        assert_eq!(schema.fields.len(), 2);
        let row = r#"{"f": [
            {
              "v": "appIsInBackground"
            },
            {
              "v": {
                "f": [
                  {
                    "v": null
                  },
                  {
                    "v": "0"
                  },
                  {
                    "v": null
                  },
                  {
                    "v": null
                  }
                ]
              }
            }
          ]
        }"#;
        let row: TableRow = serde_json::from_str(row).unwrap();
        assert_eq!(row.fields.len(), 2);
        let decoder = EventParam::create_deserialize_indices(&schema.fields).unwrap();
        assert_eq!(decoder.indices.len(), 2);
        let rec = EventParam::deserialize(row, &decoder).unwrap();
        assert_eq!(rec.key, "appIsInBackground");
        assert_eq!(rec.value.string_value, None);
        assert_eq!(rec.value.int_value, Some(0));
        assert_eq!(rec.value.float_value, None);
        assert_eq!(rec.value.double_value, None);
    }

    struct Struct3 {
        user_id: String,
        user_id_nullable: Option<String>,
        event_timestamp: i64,
        privacy_info: PrivacyInfo,
        event_params: Vec<EventParam>,
        user_properties: Vec<EventParam>,
    }

    impl Deserialize for Struct3 {
        fn create_deserialize_indices(
            schema_fields: &Vec<TableFieldSchema>,
        ) -> Result<Decoder, BigQueryError> {
            let mut indices: Vec<usize> = vec![usize::MAX; 6];
            let mut recursive_indices: Vec<Box<Decoder>> = Vec::new();
            for i in 0..3 {
                recursive_indices.push(Box::new(Decoder::default()));
            }
            for (i, field) in schema_fields.iter().enumerate() {
                if field.name == "user_id" {
                    if field.field_type != table_field_schema::Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected String type for field user_id, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[0] = i;
                } else if field.name == "event_timestamp" {
                    if field.field_type != table_field_schema::Type::Integer {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected Integer type for field event_timestamp, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[1] = i;
                } else if field.name == "user_id_nullable" {
                    if field.field_type != table_field_schema::Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected String type for field user_id_nullable, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[2] = i;
                } else if field.name == "privacy_info" {
                    if field.field_type != table_field_schema::Type::Record {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected Record type for field privacy_info, got {:?}",
                            field.field_type
                        )));
                    }
                    match &field.fields {
                        Some(fields) => {
                            let decoder = PrivacyInfo::create_deserialize_indices(&fields)?;
                            indices[3] = i;
                            recursive_indices[0] = Box::new(decoder);
                        }
                        None => {
                            return Err(BigQueryError::RowSchemaMismatch(format!(
                                "Failed to find recursive schema for field privacy_info",
                            )))
                        }
                    }
                } else if field.name == "event_params" {
                    if field.field_type != table_field_schema::Type::Record {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected Record type for field event_params, got {:?}",
                            field.field_type
                        )));
                    }
                    if field.mode != table_field_schema::Mode::Repeated {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected Repeated mode for field event_params, got {:?}",
                            field.mode
                        )));
                    }
                    match &field.fields {
                        Some(fields) => {
                            let decoder = EventParam::create_deserialize_indices(&fields)?;
                            indices[4] = i;
                            recursive_indices[1] = Box::new(decoder);
                        }
                        None => {
                            return Err(BigQueryError::RowSchemaMismatch(format!(
                                "Failed to find recursive schema for field event_params",
                            )))
                        }
                    }
                } else if field.name == "user_properties" {
                    if field.field_type != table_field_schema::Type::Record {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected Record type for field user_properties, got {:?}",
                            field.field_type
                        )));
                    }
                    if field.mode != table_field_schema::Mode::Repeated {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected Repeated mode for field user_properties, got {:?}",
                            field.mode
                        )));
                    }
                    match &field.fields {
                        Some(fields) => {
                            let decoder = EventParam::create_deserialize_indices(&fields)?;
                            indices[5] = i;
                            recursive_indices[2] = Box::new(decoder);
                        }
                        None => {
                            return Err(BigQueryError::RowSchemaMismatch(format!(
                                "Failed to find recursive schema for field user_properties",
                            )))
                        }
                    }
                }
            }
            // check that all indices are filled
            if indices[0] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'user_id' in schema".to_string(),
                ));
            }
            if indices[1] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'event_timestamp' in schema".to_string(),
                ));
            }
            if indices[2] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'user_id_nullable' in schema".to_string(),
                ));
            }
            if indices[3] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'privacy_info' in schema".to_string(),
                ));
            }
            if indices[4] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'event_params' in schema".to_string(),
                ));
            }
            if indices[5] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'user_properties' in schema".to_string(),
                ));
            }
            Ok(Decoder {
                indices,
                recursive_indices,
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
                Some(Value::String(val)) => val,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field user_id, found {:?}",
                        other_value
                    )))
                }
                None => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected required value for field user_id, found null",
                    )))
                }
            };

            let event_timestamp_idx = decoder.indices[1];
            if row.fields.len() <= event_timestamp_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: event_timestamp_idx + 1,
                    found: row.fields.len(),
                });
            }
            let event_timestamp = std::mem::take(&mut row.fields[event_timestamp_idx]);
            let event_timestamp = match event_timestamp.value {
                Some(Value::String(val)) => val.parse()?,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected integer value for field event_timestamp, found {:?}",
                        other_value
                    )))
                }
                None => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected required value for field event_timestamp, found null",
                    )))
                }
            };

            let user_id_nullable_idx = decoder.indices[2];
            if row.fields.len() <= user_id_nullable_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: user_id_nullable_idx + 1,
                    found: row.fields.len(),
                });
            }
            let user_id_nullable = std::mem::take(&mut row.fields[user_id_nullable_idx]);
            let user_id_nullable = match user_id_nullable.value {
                Some(Value::String(val)) => Some(val),
                None => None,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field user_id_nullable, found {:?}",
                        other_value
                    )))
                }
            };

            let privacy_info_idx = decoder.indices[3];
            if row.fields.len() <= privacy_info_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: privacy_info_idx + 1,
                    found: row.fields.len(),
                });
            }
            let privacy_info = std::mem::take(&mut row.fields[privacy_info_idx]);
            let privacy_info = match privacy_info.value {
                Some(Value::Record(val)) => {
                    PrivacyInfo::deserialize(val, &decoder.recursive_indices[0])?
                }
                None => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected required value for field privacy_info, found null",
                    )))
                }
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field user_id_nullable, found {:?}",
                        other_value
                    )))
                }
            };

            let event_params_idx = decoder.indices[4];
            if row.fields.len() <= event_params_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: event_params_idx + 1,
                    found: row.fields.len(),
                });
            }
            let mut event_params: Vec<EventParam> = Vec::new();
            let params = std::mem::take(&mut row.fields[event_params_idx]);
            match params.value {
                Some(Value::Array(values)) => {
                    for val in values {
                        match val.value {
                            Some(Value::Record(val)) => {
                                event_params.push(EventParam::deserialize(
                                    val,
                                    &decoder.recursive_indices[1],
                                )?);
                            }
                            other_value => {
                                return Err(BigQueryError::UnexpectedFieldType(format!(
                                    "Expected record value for items within field event_params, found {:?}",
                                    other_value
                                )))
                            }
                        }
                    }
                }
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field event_params, found {:?}",
                        other_value
                    )))
                }
                None => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected required value for field event_params, found null",
                    )))
                }
            };

            let user_properties_idx = decoder.indices[5];
            if row.fields.len() <= user_properties_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: user_properties_idx + 1,
                    found: row.fields.len(),
                });
            }
            let mut user_properties: Vec<EventParam> = Vec::new();
            let params = std::mem::take(&mut row.fields[user_properties_idx]);
            match params.value {
                Some(Value::Array(values)) => {
                    for val in values {
                        match val.value {
                            Some(Value::Record(val)) => {
                                user_properties.push(EventParam::deserialize(
                                    val,
                                    &decoder.recursive_indices[1],
                                )?);
                            }
                            other_value => {
                                return Err(BigQueryError::UnexpectedFieldType(format!(
                                    "Expected record value for items within field user_properties, found {:?}",
                                    other_value
                                )))
                            }
                        }
                    }
                }
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field user_properties, found {:?}",
                        other_value
                    )))
                }
                None => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected required value for field user_properties, found null",
                    )))
                }
            };

            Ok(Self {
                user_id,
                event_timestamp,
                user_id_nullable,
                privacy_info,
                event_params,
                user_properties,
            })
        }
    }
    #[test]
    fn it_works4() {
        let schema = r#"{
            "fields": [
              {
                "name": "user_id",
                "type": "STRING",
                "mode": "NULLABLE"
              },
              {
                "name": "user_id_nullable",
                "type": "STRING",
                "mode": "NULLABLE"
              },
              {
                "name": "event_timestamp",
                "type": "INTEGER",
                "mode": "NULLABLE"
              },
              {
                "name": "privacy_info",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                  {
                    "name": "analytics_storage",
                    "type": "STRING",
                    "mode": "NULLABLE"
                  },
                  {
                    "name": "ads_storage",
                    "type": "STRING",
                    "mode": "NULLABLE"
                  },
                  {
                    "name": "uses_transient_token",
                    "type": "STRING",
                    "mode": "NULLABLE"
                  }
                ]
              },
              {
                "name": "event_params",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                  {
                    "name": "key",
                    "type": "STRING",
                    "mode": "NULLABLE"
                  },
                  {
                    "name": "value",
                    "type": "RECORD",
                    "mode": "NULLABLE",
                    "fields": [
                      {
                        "name": "string_value",
                        "type": "STRING",
                        "mode": "NULLABLE"
                      },
                      {
                        "name": "int_value",
                        "type": "INTEGER",
                        "mode": "NULLABLE"
                      },
                      {
                        "name": "float_value",
                        "type": "FLOAT",
                        "mode": "NULLABLE"
                      },
                      {
                        "name": "double_value",
                        "type": "FLOAT",
                        "mode": "NULLABLE"
                      }
                    ]
                  }
                ]
              },
              {
                "name": "user_properties",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                  {
                    "name": "key",
                    "type": "STRING",
                    "mode": "NULLABLE"
                  },
                  {
                    "name": "value",
                    "type": "RECORD",
                    "mode": "NULLABLE",
                    "fields": [
                      {
                        "name": "string_value",
                        "type": "STRING",
                        "mode": "NULLABLE"
                      },
                      {
                        "name": "int_value",
                        "type": "INTEGER",
                        "mode": "NULLABLE"
                      },
                      {
                        "name": "float_value",
                        "type": "FLOAT",
                        "mode": "NULLABLE"
                      },
                      {
                        "name": "double_value",
                        "type": "FLOAT",
                        "mode": "NULLABLE"
                      },
                      {
                        "name": "set_timestamp_micros",
                        "type": "INTEGER",
                        "mode": "NULLABLE"
                      }
                    ]
                  }
                ]
              }
            ]
          }"#;
        let schema: TableSchema = serde_json::from_str(schema).unwrap();
        assert_eq!(schema.fields.len(), 6);
        let row = r#"{
            "f": [
              {
                "v": "user1"
              },
              {
                "v": null
              },
              {
                "v": "1648823841187011"
              },
              {
                "v": {
                  "f": [
                    {
                      "v": "Yes"
                    },
                    {
                      "v": "Yes"
                    },
                    {
                      "v": "No"
                    }
                  ]
                }
              },
              {
                "v": [
                  {
                    "v": {
                      "f": [
                        {
                          "v": "ga_session_number"
                        },
                        {
                          "v": {
                            "f": [
                              {
                                "v": null
                              },
                              {
                                "v": "216"
                              },
                              {
                                "v": null
                              },
                              {
                                "v": null
                              }
                            ]
                          }
                        }
                      ]
                    }
                  }
                ]
              },
              {
                "v": [
                  {
                    "v": {
                      "f": [
                        {
                          "v": "ga_session_id"
                        },
                        {
                          "v": {
                            "f": [
                              {
                                "v": null
                              },
                              {
                                "v": "1648823837"
                              },
                              {
                                "v": null
                              },
                              {
                                "v": null
                              },
                              {
                                "v": "1648823837891000"
                              }
                            ]
                          }
                        }
                      ]
                    }
                  }
                ]
              }
            ]
          }"#;
        let row: TableRow = serde_json::from_str(row).unwrap();
        assert_eq!(row.fields.len(), 6);
        let decoder = Struct3::create_deserialize_indices(&schema.fields).unwrap();
        assert_eq!(decoder.indices.len(), 6);
        let rec = Struct3::deserialize(row, &decoder).unwrap();
        assert_eq!(rec.user_id, "user1");
        assert_eq!(rec.event_timestamp, 1648823841187011);
        assert!(rec.user_id_nullable.is_none());
        assert_eq!(rec.privacy_info.analytics_storage, "Yes");
        assert_eq!(rec.privacy_info.ads_storage, "Yes");
        assert_eq!(rec.privacy_info.uses_transient_token, "No");
        assert_eq!(rec.event_params.len(), 1);
        assert_eq!(rec.event_params[0].value.int_value, Some(216));
        assert_eq!(rec.user_properties.len(), 1);
        assert_eq!(rec.user_properties[0].value.int_value, Some(1648823837));
    }
}
