use super::readback;
use fitz::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit, build_stream_read_with_limit,
    build_stream_rollback,
};
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestClient;
use std::time::Duration;

pub const ROUTE: &str = "stream://stress-bench/history/growing";

pub async fn request(client: &mut TestClient, frame: &[u8], kind: u16) -> Result<Vec<u8>, String> {
    tokio::time::timeout(Duration::from_secs(60), async {
        client.send_frame(frame).await.map_err(|e| e.to_string())?;
        let response = client
            .recv_frame_bytes_without_timeout()
            .await
            .map_err(|e| e.to_string())?;
        if response.len() < 5
            || response[0] != 0xFF
            || u16::from_be_bytes([response[1], response[2]]) != kind
            || usize::from(u16::from_be_bytes([response[3], response[4]])) + 5 != response.len()
        {
            return Err(format!("invalid complete Stream response type {kind}"));
        }
        Ok(response[5..].to_vec())
    })
    .await
    .map_err(|_| format!("Stream type {kind} exceeded 60 seconds; outcome may be indeterminate"))?
}

fn write_data(body: &[u8]) -> Result<Vec<u8>, String> {
    let mut decoder = PayloadDecoder::new(body);
    let status = decoder.get_u8()?;
    if status != 0 {
        if status != 2 {
            return Err("invalid versioned Stream error".into());
        }
        let code = decoder.get_u32()?;
        let detail = decoder.get_string_ref()?;
        complete(&decoder)?;
        return Err(format!("Stream error code {code}: {detail}"));
    }
    let data = decoder.get_bytes()?.to_vec();
    complete(&decoder)?;
    Ok(data)
}

fn complete(decoder: &PayloadDecoder<'_>) -> Result<(), String> {
    if decoder.is_complete() {
        Ok(())
    } else {
        Err("trailing Stream response data".into())
    }
}

pub async fn begin(client: &mut TestClient) -> Result<u64, String> {
    let body = request(client, &build_stream_begin(ROUTE), 600).await?;
    let mut decoder = PayloadDecoder::new(&body);
    if decoder.get_u8()? != 0 {
        return Err(format!("Stream BEGIN rejected: {body:?}"));
    }
    let session = decoder.get_u64()?;
    if !decoder.get_bytes()?.is_empty() {
        return Err("unexpected BEGIN data".into());
    }
    complete(&decoder)?;
    Ok(session)
}

pub async fn append(
    client: &mut TestClient,
    session: u64,
    offset: u64,
    bytes: usize,
) -> Result<(), String> {
    let body = request(
        client,
        &build_stream_append(session, offset, &readback::payload(offset, bytes)),
        601,
    )
    .await?;
    let data = write_data(&body)?;
    let mut decoder = PayloadDecoder::new(&data);
    if decoder.get_u64()? != offset {
        return Err("APPEND assigned a different offset".into());
    }
    complete(&decoder)
}

pub async fn commit(client: &mut TestClient, session: u64) -> Result<(), String> {
    let body = request(client, &build_stream_commit(session, 1), 602).await?;
    if !write_data(&body)?.is_empty() {
        return Err("unexpected COMMIT data".into());
    }
    Ok(())
}

pub async fn rollback(client: &mut TestClient, session: u64) -> Result<(), String> {
    let body = request(client, &build_stream_rollback(session), 603).await?;
    if !write_data(&body)?.is_empty() {
        return Err("unexpected ROLLBACK data".into());
    }
    Ok(())
}

pub async fn replay(client: &mut TestClient, committed: u64, bytes: usize) -> Result<u64, String> {
    tokio::time::timeout(
        Duration::from_secs(600),
        replay_pages(client, committed, bytes),
    )
    .await
    .map_err(|_| "Stream complete replay exceeded 600 seconds".to_owned())?
}

async fn replay_pages(
    client: &mut TestClient,
    committed: u64,
    bytes: usize,
) -> Result<u64, String> {
    // Stay below the u16 TCP response envelope at every supported payload size.
    let limit =
        u64::try_from((60_000 / (bytes + ROUTE.len() + 128)).min(32)).map_err(|e| e.to_string())?;
    let mut start = 0;
    loop {
        let body = request(
            client,
            &build_stream_read_with_limit(ROUTE, start, limit),
            604,
        )
        .await?;
        let count = readback::verify_page(&body, ROUTE, start, committed, limit, bytes)?;
        if count == 0 {
            return Ok(start);
        }
        start += count;
    }
}
