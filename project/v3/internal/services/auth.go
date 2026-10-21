package services

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type AuthService struct {
	secret string
	users  map[string]string
}

func newAuthService(secret string, users map[string]string) *AuthService {
	if secret == "" {
		secret = "oh-boy-this-is-not-ideal"
	}
	if users == nil {
		users = map[string]string{}
	}
	return &AuthService{
		secret: secret,
		users:  users,
	}
}

func (a *AuthService) CheckPassword(username string, password string) bool {
	expected, ok := a.users[username]
	if !ok {
		return false
	}
	return expected == password
}

func (a *AuthService) issueToken(username string) (string, error) {
	if username == "" {
		return "", errors.New("username required")
	}

	now := time.Now().Unix()
	exp := now + 3600

	// For building out the core of a JWT string
	claims := map[string]any{
		"sub": username,
		"iat": now,
		"exp": exp,
	}

	payloadInBytes, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	payloadEncoded := base64.RawURLEncoding.EncodeToString(payloadInBytes)
	signature := signToken(a.secret, payloadEncoded)

	return payloadEncoded + "." + signature, nil
}

// Eventually I learned you can comma the params like (secret, payload string) but leaving it as an artifact to my own verbosity. Claude has nothing on me
func signToken(secret string, payload string) string {
	code := hmac.New(sha256.New, []byte(secret))
	code.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(code.Sum(nil))
}

func (a *AuthService) ValidateToken(token string) (string, bool) {
	// Empty tokens
	if token == "" {
		return "", false
	}

	// Malformed tokens
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return "", false
	}

	payloadEncoded, signature := parts[0], parts[1]
	expectedSignature := signToken(a.secret, payloadEncoded)

	// Mismatched token
	if !hmac.Equal([]byte(expectedSignature), []byte(signature)) {
		return "", false
	}

	// Decoding error, another possibly malformed token
	payloadInBytes, err := base64.RawURLEncoding.DecodeString(payloadEncoded)
	if err != nil {
		return "", false
	}

	var claims map[string]any
	if err := json.Unmarshal(payloadInBytes, &claims); err != nil {
		return "", false
	}

	// Check for expired tokens. Learned some neat syntax for a type assertion here, kinda cool
	exp, ok := claims["exp"].(float64)
	if ok && time.Now().Unix() > int64(exp) {
		return "", false
	}

	// Quick sanity check that there is in fact a subject this token is assigned to
	username, _ := claims["sub"].(string)
	if username == "" {
		return "", false
	}

	return username, true
}
