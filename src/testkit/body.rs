use bytes::Bytes;
use http_body_util::BodyExt;

pub async fn to_bytes<B>(body: B) -> Result<Bytes, B::Error>
where
    B: BodyExt + Unpin,
{
    body.collect()
        .await
        .map(http_body_util::Collected::to_bytes)
}
