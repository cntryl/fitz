pub fn parse_pairs(raw: &str) -> Result<usize, String> {
    let pairs = raw.parse::<usize>().map_err(|error| error.to_string())?;
    if !(1..=1000).contains(&pairs) {
        return Err("FITZ_QUEUE_DRAIN_PAIRS must be in 1..=1000".into());
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::parse_pairs;

    #[test]
    fn should_accept_the_finite_pair_cap() {
        // Arrange
        let raw = "1000";
        // Act
        let parsed = parse_pairs(raw);
        // Assert
        assert_eq!(parsed, Ok(1000));
    }

    #[test]
    fn should_reject_work_outside_the_declared_pair_cap() {
        // Arrange
        let values = ["0", "1001", "18446744073709551616", "bad"];
        // Act
        let results = values.map(parse_pairs);
        // Assert
        assert!(results.iter().all(Result::is_err));
    }
}
