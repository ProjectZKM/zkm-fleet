//! Lossless JSON codec for application-owned protobuf extension fields.
use std::ops::{Deref, DerefMut};

use prost::{
    bytes::{Buf, BufMut},
    encoding::{DecodeContext, WireType},
    DecodeError, Message,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Json(pub Value);

impl From<Value> for Json {
    fn from(value: Value) -> Self {
        Self(value)
    }
}
impl From<Json> for Value {
    fn from(value: Json) -> Self {
        value.0
    }
}
impl Deref for Json {
    type Target = Value;
    fn deref(&self) -> &Value {
        &self.0
    }
}
impl DerefMut for Json {
    fn deref_mut(&mut self) -> &mut Value {
        &mut self.0
    }
}
impl PartialEq<Value> for Json {
    fn eq(&self, other: &Value) -> bool {
        self.0 == *other
    }
}

impl Message for Json {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        prost::encoding::bytes::encode(1, &self.0.to_string().into_bytes(), buf);
    }
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        if tag != 1 {
            return prost::encoding::skip_field(wire_type, tag, buf, ctx);
        }
        let mut bytes = Vec::new();
        prost::encoding::bytes::merge(wire_type, &mut bytes, buf, ctx)?;
        self.0 = serde_json::from_slice(&bytes).map_err(|e| DecodeError::new(e.to_string()))?;
        Ok(())
    }
    fn encoded_len(&self) -> usize {
        prost::encoding::bytes::encoded_len(1, &self.0.to_string().into_bytes())
    }
    fn clear(&mut self) {
        self.0 = Value::Null;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn protobuf_json_preserves_large_integers_and_rejects_invalid_json() {
        let value = serde_json::json!({"integer":u64::MAX,"array":[true,null,"value"]});
        let json = Json::from(value.clone());
        assert_eq!(
            Json::decode(json.encode_to_vec().as_slice()).unwrap().0,
            value
        );
        let mut bad = Vec::new();
        prost::encoding::bytes::encode(1, &b"not json".to_vec(), &mut bad);
        assert!(Json::decode(bad.as_slice()).is_err());
    }
}
