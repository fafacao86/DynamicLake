use crate::NodeId;

use super::{MetastoreError, MetastoreResult};

pub fn mv_definition_key(node_id: &NodeId) -> MetastoreResult<Vec<u8>> {
    key(&["ivm", "mv", &node_id.to_string()])
}

pub fn mv_definitions_prefix() -> MetastoreResult<Vec<u8>> {
    let mut prefix = key(&["ivm", "mv"])?;
    prefix.push(b'/');
    Ok(prefix)
}

pub fn stream_object_key(node_id: &NodeId, stream_name: &str) -> MetastoreResult<Vec<u8>> {
    key(&["ivm", "stream", &node_id.to_string(), stream_name])
}

pub fn stream_objects_prefix(node_id: &NodeId) -> MetastoreResult<Vec<u8>> {
    let mut prefix = key(&["ivm", "stream", &node_id.to_string()])?;
    prefix.push(b'/');
    Ok(prefix)
}

fn key(parts: &[&str]) -> MetastoreResult<Vec<u8>> {
    if parts.is_empty() {
        return Err(MetastoreError::InvalidKey(
            "key must contain at least one part".to_string(),
        ));
    }
    for part in parts {
        validate_key_part(part)?;
    }
    Ok(parts
        .iter()
        .map(|part| hex_encode(part.as_bytes()))
        .collect::<Vec<_>>()
        .join("/")
        .into_bytes())
}

fn validate_key_part(part: &str) -> MetastoreResult<()> {
    if part.is_empty() {
        return Err(MetastoreError::InvalidKey(
            "key parts must not be empty".to_string(),
        ));
    }
    if let Some(character) = part
        .chars()
        .find(|character| is_invalid_key_character(*character))
    {
        return Err(MetastoreError::InvalidKey(format!(
            "key part `{part}` contains disallowed character {character:?}"
        )));
    }
    Ok(())
}

fn is_invalid_key_character(character: char) -> bool {
    matches!(
        character,
        '/' | '\\' | '<' | '>' | ':' | '"' | '|' | '?' | '*'
    ) || character.is_control()
}

fn hex_encode(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_rejects_disallowed_characters() {
        for part in [
            "bad/name",
            "bad\\name",
            "bad:name",
            "bad*name",
            "bad\nname",
            "bad\0name",
            "订单/明细",
        ] {
            let error = key(&[part]).expect_err("key part should be rejected");
            assert!(matches!(error, MetastoreError::InvalidKey(_)));
        }
    }

    #[test]
    fn key_accepts_supported_characters() {
        assert_eq!(
            key(&["ivm", "订单-1_test.name", "0"]).expect("valid key"),
            b"69766d/e8aea2e58d952d315f746573742e6e616d65/30"
        );
    }
}
