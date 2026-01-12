use crate::domains::schedule::protocol::SchedulePayload;

#[test]
fn should_roundtrip_schedule_tlv() {
    let payload = SchedulePayload {
        cron: "* * * * *".to_string(),
        resource: "res1".to_string(),
        operation: "op".to_string(),
    };

    let enc = payload.encode();
    let dec = SchedulePayload::decode(&enc).unwrap();
    assert_eq!(dec, payload);
}

#[test]
fn should_reject_missing_fields() {
    // Missing fields should return Err
    // Only cron field (type 1)
    use crate::protocol::tlv::{TlvEncoder, MessageType};
    let mut enc = TlvEncoder::new();
    enc.encode(MessageType(1), b"* * * * *");
    let data = enc.finish();
    let res = SchedulePayload::decode(&data);
    assert!(res.is_err());
}
