#[derive(thiserror::Error, Debug)]
pub enum BigQueryError {
    #[error("Authentication error (error: {0})")]
    YupAuthError(#[from] yup_oauth2::Error),
    #[error("Request to google api error (error: {0})")]
    ApiRequestError(#[from] reqwest::Error),
    #[error("Malformed google api response: missing job_id")]
    MissingJobIdInGoogleApiResponse,
}
