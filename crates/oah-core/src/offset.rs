use crate::error::{CoreError, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Stream offset: `-1` at the origin, otherwise `%016d_%016d` (batch, index).
/// Reads are exclusive of the named offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamOffset {
    pub batch: i64,
    pub index: i64,
}

impl StreamOffset {
    pub const ORIGIN: Self = Self {
        batch: -1,
        index: -1,
    };

    pub fn new(batch: i64, index: i64) -> Self {
        Self { batch, index }
    }

    pub fn is_origin(self) -> bool {
        self.batch < 0
    }

    pub fn parse(raw: &str) -> Result<Self> {
        if raw == "-1" {
            return Ok(Self::ORIGIN);
        }
        let (batch, index) = raw.split_once('_').ok_or_else(|| {
            CoreError::InvalidId(format!("offset must be -1 or %016d_%016d, got {raw}"))
        })?;
        let batch = batch.parse::<i64>().map_err(|_| {
            CoreError::InvalidId(format!("malformed offset batch in {raw}"))
        })?;
        let index = index.parse::<i64>().map_err(|_| {
            CoreError::InvalidId(format!("malformed offset index in {raw}"))
        })?;
        Ok(Self { batch, index })
    }

    pub fn after_batch(batch_seq: i64) -> Self {
        Self {
            batch: batch_seq,
            index: 0,
        }
    }
}

impl fmt::Display for StreamOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_origin() {
            f.write_str("-1")
        } else {
            write!(f, "{:016}_{:016}", self.batch, self.index)
        }
    }
}

impl Serialize for StreamOffset {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for StreamOffset {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn origin_and_roundtrip() {
        assert_eq!(StreamOffset::ORIGIN.to_string(), "-1");
        let off = StreamOffset::new(2, 0);
        assert_eq!(off.to_string(), "0000000000000002_0000000000000000");
        assert_eq!(StreamOffset::parse(&off.to_string()).unwrap(), off);
    }
}
