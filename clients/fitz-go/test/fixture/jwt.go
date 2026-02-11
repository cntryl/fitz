package fixture

// GenerateTestJWT creates a JWT token for testing.
// For auth-disabled mode, this is not called.
// For auth-enabled mode, this should generate a proper JWT with clientID/clientSecret.
func GenerateTestJWT(clientID, clientSecret string) (string, error) {
	// TODO: Implement proper JWT generation when FITZ_AUTH_REQUIRED=true
	// For now, return a dummy token that auth-disabled broker will accept
	return "test.jwt.token", nil
}
