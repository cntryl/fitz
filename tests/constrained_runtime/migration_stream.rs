//! Public-wire Stream replay for the migration rehearsal. Replay creates new
//! timestamps; deployment-specific TTL reconciliation is deliberately separate.
use super::*;
use fitz::protocol::payload_codec::PayloadDecoder;

const STREAM_ROUTE: &str = "stream://migration/reference/history";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(super) struct Event {
    body: Vec<u8>,
    metadata: Option<Vec<u8>>,
}

pub(super) fn fixture() -> Vec<Event> {
    (0_u8..8)
        .map(|index| Event {
            body: vec![index; 32 * 1024],
            metadata: Some(vec![index, 0, 255]),
        })
        .collect()
}

pub(super) async fn replay(broker: &Broker, events: &[Event]) {
    let mut client = connect(broker.address()).await;
    let started = request(&mut client, &build_stream_begin(STREAM_ROUTE), 600).await;
    let session = parse_stream_session_id(&started).unwrap();
    for (offset, event) in events.iter().enumerate() {
        request(
            &mut client,
            &build_stream_append_with_metadata(
                session,
                u64::try_from(offset).unwrap(),
                &event.body,
                event.metadata.as_deref(),
            ),
            601,
        )
        .await;
    }
    let mut payload = session.to_be_bytes().to_vec();
    payload.push(1); // Explicit Sync in both released wire generations.
    let mut commit = FrameBuilder::new();
    commit.encode_field(602, &payload);
    request(&mut client, &commit.build(), 602).await;
    client.close().await.unwrap();
}

pub(super) async fn export(broker: &Broker) -> Vec<Event> {
    let mut client = connect(broker.address()).await;
    let mut events = Vec::new();
    // The TCP response-byte bound can split this fixture across multiple pages.
    for _ in 0..=8 {
        let offset = u64::try_from(events.len()).unwrap();
        let reply = request(&mut client, &build_stream_read(STREAM_ROUTE, offset), 604).await;
        let mut outer = PayloadDecoder::new(&reply);
        assert_eq!(outer.get_u8().unwrap(), 0);
        assert!(outer.get_optional_u64().unwrap().is_none());
        let data = outer.get_bytes().unwrap();
        assert!(outer.is_complete());
        let mut decoder = PayloadDecoder::new(&data);
        let count = decoder.get_u32().unwrap();
        for index in 0..count {
            assert_eq!(decoder.get_string_ref().unwrap(), STREAM_ROUTE);
            assert_eq!(decoder.get_u8().unwrap(), 0);
            assert_eq!(decoder.get_u64().unwrap(), offset + u64::from(index));
            decoder.get_optional_u64().unwrap();
            decoder.get_optional_u64().unwrap();
            events.push(Event {
                body: decoder.get_bytes().unwrap().to_vec(),
                metadata: decoder
                    .get_optional_bytes()
                    .unwrap()
                    .map(|bytes| bytes.to_vec()),
            });
            assert!(decoder.get_u64().unwrap() > 0);
        }
        let last = decoder.get_u64().unwrap();
        if count > 0 {
            assert_eq!(last, offset + u64::from(count) - 1);
        }
        decoder.get_optional_u64().unwrap();
        decoder.get_optional_u64().unwrap();
        let more = decoder.get_u8().unwrap();
        assert!(more <= 1);
        assert!(decoder.is_complete());
        if more == 0 {
            client.close().await.unwrap();
            return events;
        }
        assert!(count > 0, "continuation must make progress");
    }
    panic!("Stream migration export exceeded the eight-event fixture");
}
