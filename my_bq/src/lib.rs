pub mod client;
pub mod error;
pub mod structs;

pub use error::BigQueryError;
pub use my_bq_proc::Deserialize;
pub use structs::table_row::TableRow;

extern crate self as my_bq;
