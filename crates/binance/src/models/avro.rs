use avro_rs::{from_value, Reader, Schema, Writer};
use serde::{de::DeserializeOwned, Serialize};

pub trait Avro: Serialize + DeserializeOwned {
    fn raw_schema() -> &'static str;

    #[allow(clippy::missing_errors_doc)]
    fn serialize_to_avro(&self) -> Result<Vec<u8>, avro_rs::Error> {
        let schema = Schema::parse_str(Self::raw_schema())?;
        let mut writer = Writer::new(&schema, Vec::new());
        let _unused = writer.append_ser(self)?;
        writer.into_inner()
    }

    #[allow(clippy::missing_errors_doc)]
    fn deserialize_from_avro(bytes: &[u8]) -> Result<Self, avro_rs::Error> {
        let schema = Schema::parse_str(Self::raw_schema())?;
        let reader = Reader::with_schema(&schema, bytes)?;

        // Get the first record
        if let Some(value) = reader.into_iter().next() {
            let value = value?;
            from_value::<Self>(&value)
        } else {
            Err(avro_rs::Error::DeserializeValue(String::from("Empty value")))
        }
    }
}
