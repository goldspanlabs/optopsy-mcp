//! Serde helpers that (de)serialize `NaiveDateTime` as UTC epoch seconds (i64).
//!
//! Attach to fields with `#[serde(with = "epoch_serde")]`.

use chrono::NaiveDateTime;
use serde::{self, Deserialize, Deserializer, Serializer};

pub fn serialize<S>(dt: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_i64(dt.and_utc().timestamp())
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let epoch = i64::deserialize(deserializer)?;
    chrono::DateTime::from_timestamp(epoch, 0)
        .map(|dt| dt.naive_utc())
        .ok_or_else(|| serde::de::Error::custom(format!("invalid epoch timestamp: {epoch}")))
}
