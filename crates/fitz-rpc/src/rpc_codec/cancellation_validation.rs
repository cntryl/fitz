use super::*;

/// A request payload up to, but not including, the budget presence byte.
fn request_payload_before_budget() -> Vec<u8> {
    let request = RpcRequest::new(
        RouteFamily::new(1),
        Uuid::new_v4(),
        Route::new("rpc://realm/area/resource"),
        Bytes::from_static(b"body"),
    );
    let mut payload = encode_request_into(&request, &mut PayloadEncoder::new());
    payload.pop();
    payload
}

#[test]
fn should_reject_malformed_remaining_budget_fields() {
    // Arrange
    let tails = [
        vec![2, 0, 0, 0, 1],
        vec![1],
        vec![1, 0, 0, 0, 1, 0],
        [vec![1], (MAX_RPC_BUDGET_MILLIS + 1).to_be_bytes().to_vec()].concat(),
        vec![0, 0],
        vec![],
    ];

    // Act
    let invalid = tails.map(|tail| {
        let mut payload = request_payload_before_budget();
        payload.extend(tail);
        extract_request_remaining_budget_ms(&payload).is_err()
    });

    // Assert
    assert_eq!(invalid, [true; 6]);
}

#[test]
fn should_accept_remaining_budget_boundaries() {
    // Arrange
    let budgets = [0, MAX_RPC_BUDGET_MILLIS];

    // Act
    let actual = budgets.map(|budget| {
        let mut payload = request_payload_before_budget();
        payload.push(1);
        payload.extend(budget.to_be_bytes());
        extract_request_remaining_budget_ms(&payload).expect("valid boundary")
    });

    // Assert
    assert_eq!(actual, [Some(0), Some(MAX_RPC_BUDGET_MILLIS)]);
}

#[test]
fn should_reject_invalid_caller_cancellation_payloads() {
    // Arrange
    let id = Uuid::new_v4();
    let malformed = [
        vec![],
        vec![1],
        [vec![1], id.as_bytes().to_vec(), vec![3]].concat(),
        [vec![2], id.as_bytes().to_vec(), vec![1]].concat(),
        [vec![3], id.as_bytes().to_vec(), vec![0]].concat(),
    ];

    // Act
    let invalid = malformed.map(|payload| parse_cancellation_message(&payload).is_err());

    // Assert
    assert_eq!(invalid, [true; 5]);
}

#[test]
fn should_reject_invalid_registration_flags() {
    // Arrange
    let tails = [vec![2], vec![], vec![1, 0]];

    // Act
    let invalid = tails.map(|tail| {
        let mut payload = BytesMut::new();
        put_payload_string(&mut payload, "rpc://realm/area/resource");
        payload.put_u32(1);
        payload.extend(tail);
        extract_registration_cancellation_support(&payload).is_err()
    });

    // Assert
    assert_eq!(invalid, [true; 3]);
}
