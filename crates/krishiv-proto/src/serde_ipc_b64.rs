//! Serde adapter for Arrow IPC payload lists on JSON wire types.
//!
//! `Vec<Vec<u8>>` under plain serde_json renders each byte as a decimal
//! number (`[[137,65,82,...]]`) — roughly 3.7x the raw payload size plus a
//! per-byte serialize/parse cost, paid on every drain and batch-SQL poll.
//! This adapter encodes each payload as a base64 string instead, matching
//! the `input_batches_b64` convention the request side already uses.
//!
//! Apply with `#[serde(with = "krishiv_proto::serde_ipc_b64")]` on both the
//! serializing (coordinator) and deserializing (client) struct so the wire
//! shape changes in lockstep; a version-skewed peer fails loudly with a type
//! error instead of silently misreading bytes.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

pub fn serialize<S: serde::Serializer>(
    payloads: &[Vec<u8>],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(payloads.iter().map(|bytes| STANDARD.encode(bytes)))
}

pub fn deserialize<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<Vec<u8>>, D::Error> {
    use serde::Deserialize as _;
    let encoded = Vec::<String>::deserialize(deserializer)?;
    encoded
        .iter()
        .map(|value| STANDARD.decode(value).map_err(serde::de::Error::custom))
        .collect()
}

#[cfg(test)]
mod tests {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Wire {
        #[serde(with = "super")]
        payloads: Vec<Vec<u8>>,
    }

    #[test]
    fn serializes_as_base64_strings_not_integer_arrays() {
        let json = serde_json::to_string(&Wire {
            payloads: vec![vec![1, 2, 3]],
        })
        .expect("serialize");
        assert_eq!(json, r#"{"payloads":["AQID"]}"#);
    }

    #[test]
    fn round_trips_and_rejects_invalid_base64() {
        let wire: Wire = serde_json::from_str(r#"{"payloads":["AQID",""]}"#).expect("deserialize");
        assert_eq!(wire.payloads, vec![vec![1, 2, 3], vec![]]);
        assert!(serde_json::from_str::<Wire>(r#"{"payloads":["@@@"]}"#).is_err());
        // The old integer-array shape must fail loudly, not decode wrongly.
        assert!(serde_json::from_str::<Wire>(r#"{"payloads":[[1,2,3]]}"#).is_err());
    }
}
