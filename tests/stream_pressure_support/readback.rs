use fitz::protocol::payload_codec::PayloadDecoder;

pub fn payload(offset: u64, bytes: usize) -> Vec<u8> {
    let mut value = vec![0xB6; bytes];
    value[..8].copy_from_slice(&offset.to_be_bytes());
    value
}

pub fn verify_page(
    body: &[u8],
    route: &str,
    start: u64,
    committed: u64,
    limit: u64,
    payload_bytes: usize,
) -> Result<u64, String> {
    let mut outer = PayloadDecoder::new(body);
    if outer.get_u8()? != 0 || outer.get_optional_u64()?.is_some() {
        return Err("replay was rejected or unexpectedly owns a session".into());
    }
    let data = outer.get_bytes()?;
    complete(&outer)?;
    let mut decoder = PayloadDecoder::new(&data);
    let count = u64::from(decoder.get_u32()?);
    let end = start.checked_add(count).ok_or("replay offset overflow")?;
    if count > limit || end > committed || (count == 0 && start < committed) {
        return Err(format!(
            "replay count {count} at {start} contradicts {committed} committed events"
        ));
    }
    for offset in start..end {
        let found_route = decoder.get_string_ref()?;
        let kind = decoder.get_u8()?;
        let resource_offset = decoder.get_u64()?;
        decoder.get_optional_u64()?;
        decoder.get_optional_u64()?;
        let body = decoder.get_bytes()?;
        let metadata = decoder.get_optional_bytes()?;
        decoder.get_u64()?;
        if found_route != route
            || kind != 0
            || resource_offset != offset
            || body.as_ref() != payload(offset, payload_bytes)
            || metadata.is_some()
        {
            return Err(format!(
                "replay changed route, offset or bytes at event {offset}"
            ));
        }
    }
    let last_offset = decoder.get_u64()?;
    decoder.get_optional_u64()?;
    decoder.get_optional_u64()?;
    let has_more = decoder.get_u8()?;
    complete(&decoder)?;
    if (count > 0 && last_offset != end - 1) || has_more != u8::from(end < committed) {
        return Err("replay cursor contradicts the committed history".into());
    }
    Ok(count)
}

fn complete(decoder: &PayloadDecoder<'_>) -> Result<(), String> {
    if decoder.is_complete() {
        Ok(())
    } else {
        Err("trailing replay data".into())
    }
}
