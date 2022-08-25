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
            let mut query_results: QueryResults = res.json().await?;
            let indices = T::create_deserialize_indices(&query_results.schema.fields)?;
            let mut result: Vec<T> = query_results
                .rows
                .into_iter()
                .map(|row| T::deserialize(row, &indices))
                .collect::<Result<Vec<T>, BigQueryError>>()?;
            while let Some(page_token) = &query_results.page_token {
                let api_url = format!(
                    "https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries/{job_id}?pageToken={page_token}",
                    project_id = self.project_id,
                    job_id = job_id,
                    page_token = page_token,
                );
                let res = self
                    .inner_client
                    .reqwest_client
                    .get(api_url)
                    .bearer_auth(tok.as_str())
                    .send()
                    .await?;
                query_results = res.json().await?;
                let indices = T::create_deserialize_indices(&query_results.schema.fields)?;
                let result2: Vec<T> = query_results
                    .rows
                    .into_iter()
                    .map(|row| T::deserialize(row, &indices))
                    .collect::<Result<Vec<T>, BigQueryError>>()?;
                result.extend(result2);
            }
            Ok(result)
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
        #[my_bq(rename = "analytics_storage")]
        analytics: String,
        #[my_bq(rename = "ads_storage")]
        ads: String,
        #[my_bq(rename = "int_value")]
        int_val: i64,
        #[my_bq(rename = "optional_int_value")]
        optional_int_val: Option<i64>,
    }

    #[test]
    fn test_simplest_struct() {
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
                "name": "int_value",
                "type": "INTEGER",
                "mode": "NULLABLE"
                },
                {
                "name": "optional_int_value",
                "type": "INTEGER",
                "mode": "NULLABLE"
                }
            ]
          }"#;
        let schema: TableSchema = serde_json::from_str(schema).unwrap();
        assert_eq!(schema.fields.len(), 4);
        {
            let row = r#"{"f": [
                {
                "v": "Yes"
                },
                {
                "v": "Yes2"
                },
                {
                "v": "13337"
                },
                {
                "v": null
                }
            ]
            }"#;
            let row: TableRow = serde_json::from_str(row).unwrap();
            assert_eq!(row.fields.len(), 4);
            let decoder = MyStruct2::create_deserialize_indices(&schema.fields).unwrap();
            assert_eq!(decoder.indices.len(), 4);
            let rec = MyStruct2::deserialize(row, &decoder).unwrap();
            assert_eq!(rec.analytics, "Yes");
            assert_eq!(rec.ads, "Yes2");
            assert_eq!(rec.int_val, 13337);
            assert_eq!(rec.optional_int_val, None);
        }
        {
            let row = r#"{"f": [
                {
                "v": "Yes"
                },
                {
                "v": "Yes2"
                },
                {
                "v": "13337"
                },
                {
                "v": "13338"
                }
            ]
            }"#;
            let row: TableRow = serde_json::from_str(row).unwrap();
            assert_eq!(row.fields.len(), 4);
            let decoder = MyStruct2::create_deserialize_indices(&schema.fields).unwrap();
            assert_eq!(decoder.indices.len(), 4);
            let rec = MyStruct2::deserialize(row, &decoder).unwrap();
            assert_eq!(rec.analytics, "Yes");
            assert_eq!(rec.ads, "Yes2");
            assert_eq!(rec.int_val, 13337);
            assert_eq!(rec.optional_int_val, Some(13338));
        }
    }

    #[derive(Deserialize)]
    struct PrivacyInfo {
        analytics_storage: String,
        ads_storage: String,
        uses_transient_token: String,
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

    #[derive(Deserialize)]
    struct JsonValue {
        string_value: Option<String>,
        int_value: Option<i64>,
        float_value: Option<f64>,
        double_value: Option<f64>,
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

    #[derive(Deserialize)]
    struct EventParam {
        key: String,
        value: JsonValue,
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

    #[derive(Deserialize)]
    struct Struct3 {
        user_id: String,
        user_id_nullable: Option<String>,
        event_timestamp: i64,
        privacy_info: PrivacyInfo,
        event_params: Vec<EventParam>,
        user_properties: Vec<EventParam>,
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
