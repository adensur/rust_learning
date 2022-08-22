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

    struct Struct3 {
        user_id: String,
        user_id_nullable: Option<String>,
        event_timestamp: i64,
        privacy_info: PrivacyInfo,
    }

    impl Deserialize for Struct3 {
        fn create_deserialize_indices(
            schema_fields: &Vec<TableFieldSchema>,
        ) -> Result<Decoder, BigQueryError> {
            let mut indices: Vec<usize> = vec![usize::MAX; 4];
            let mut recursive_indices: Vec<Box<Decoder>> = Vec::new();
            for i in 0..1 {
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

            Ok(Self {
                user_id,
                event_timestamp,
                user_id_nullable,
                privacy_info,
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
              }
            ]
          }"#;
        let schema: TableSchema = serde_json::from_str(schema).unwrap();
        assert_eq!(schema.fields.len(), 4);
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
              }
            ]
          }"#;
        let row: TableRow = serde_json::from_str(row).unwrap();
        assert_eq!(row.fields.len(), 4);
        let decoder = Struct3::create_deserialize_indices(&schema.fields).unwrap();
        assert_eq!(decoder.indices.len(), 4);
        let rec = Struct3::deserialize(row, &decoder).unwrap();
        assert_eq!(rec.user_id, "user1");
        assert_eq!(rec.event_timestamp, 1648823841187011);
        assert!(rec.user_id_nullable.is_none());
        assert_eq!(rec.privacy_info.analytics_storage, "Yes");
        assert_eq!(rec.privacy_info.ads_storage, "Yes");
        assert_eq!(rec.privacy_info.uses_transient_token, "No");
    }
}
