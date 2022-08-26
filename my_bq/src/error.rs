use std::num::{ParseFloatError, ParseIntError};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum BigQueryError {
    #[error("Authentication error (error: {0})")]
    YupAuthError(#[from] yup_oauth2::Error),
    #[error("Serde json (error: {0})")]
    JsonDeserializationError(#[from] serde_json::Error),
    #[error("Int conversion error (error: {0})")]
    IntConversionError(#[from] ParseIntError),
    #[error("Tokio join error (error: {0})")]
    TokioJoinError(#[from] tokio::task::JoinError),
    #[error("Float conversion error (error: {0})")]
    FloatConversionError(#[from] ParseFloatError),
    #[error("Request to google api error (error: {0})")]
    ApiRequestError(#[from] reqwest::Error),
    #[error("Malformed google api response: missing job_id")]
    MissingJobIdInGoogleApiResponse,
    #[error("Malformed google api response: missing rows")]
    MissingRowsInQueryResponse,
    #[error("Malformed google api response: missing schema")]
    MissingSchemaInQueryResponse,
    #[error("Malformed google api response: missing total_rows field")]
    MissingTotalRowsInQueryResponse,
    #[error("Malformed google api response: expected fields len {expected}, found {found}")]
    NotEnoughFields { expected: usize, found: usize },
    #[error("Malformed google api response: {0}")]
    UnexpectedFieldType(String),
    #[error("Struct deserialization error due to schema mismatch: {0}")]
    RowSchemaMismatch(String),
    #[error(" while running BigQuery job: {msg}")]
    JobInsertError { msg: String },
    #[error("Job is not complete yet")]
    JobPending,
}

//unsafe impl Send for BigQueryError {}
