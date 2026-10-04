use super::*;

fn request_payload() -> Vec<u8> {
    let request = RpcRequest::new(
        RouteFamily::new(1),
        Uuid::new_v4(),
        Route::new("rpc://realm/area/resource"),
        Bytes::from_static(b"body"),
    );
    encode_request_into(&request, &mut PayloadEncoder::new())
}

#[test]
fn should_reject_malformed_remaining_budget_extensions() {
    // Arrange
    let extensions = [
        vec![2, 1, 0, 0, 0, 1],
        vec![1, 2, 0, 0, 0, 1],
        vec![1, 1],
        vec![1, 1, 0, 0, 0, 1, 0],
        [
            vec![1, 1],
            (MAX_RPC_BUDGET_MILLIS + 1).to_be_bytes().to_vec(),
        ]
        .concat(),
    ];

    // Act
    let invalid = extensions.map(|extension| {
        let mut payload = request_payload();
        payload.extend(extension);
        extract_request_remaining_budget_ms(&payload).is_err()
    });

    // Assert
    assert_eq!(invalid, [true; 5]);
}

#[test]
fn should_accept_zero_and_maximum_remaining_budget() {
    // Arrange
    let budgets = [0, MAX_RPC_BUDGET_MILLIS];

    // Act
    let actual = budgets.map(|budget| {
        let mut payload = request_payload();
        payload.extend([1, 1]);
        payload.extend(budget.to_be_bytes());
        extract_request_remaining_budget_ms(&payload).expect("valid boundary")
    });

    // Assert
    assert_eq!(actual, [Some(0), Some(MAX_RPC_BUDGET_MILLIS)]);
}

#[test]
fn should_reject_worker_only_reasons_and_trailing_cancellation_bytes() {
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
fn should_reject_unknown_registration_flags_and_trailing_extension_bytes() {
    // Arrange
    let extensions = [vec![2, 1], vec![1, 2], vec![1], vec![1, 1, 0]];

    // Act
    let invalid = extensions.map(|extension| {
        let mut payload = BytesMut::new();
        put_payload_string(&mut payload, "rpc://realm/area/resource");
        payload.put_u32(1);
        payload.extend(extension);
        extract_registration_cancellation_support(&payload).is_err()
    });

    // Assert
    assert_eq!(invalid, [true; 4]);
}
