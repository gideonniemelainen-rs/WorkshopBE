package services

import (
	"testing"
)

func TestAuthServiceCheckPassword(t *testing.T) {
	a := newAuthService("test-secret", map[string]string{"admin": "password123"})

	if !a.CheckPassword("admin", "password123") {
		t.Fatalf("expected password check to succeed")
	}
	if a.CheckPassword("admin", "lololol") {
		t.Fatalf("expected password check to fail for incorrect password")
	}
	if a.CheckPassword("missing", "password123") {
		t.Fatalf("expected password check to fail for missing user")
	}
}

func TestAuthServiceIssueAndValidateToken(t *testing.T) {
	auth := newAuthService("test-secret", nil)

	token, err := auth.issueToken("admin")
	if err != nil {
		t.Fatalf("issue token returned error: %v", err)
	}

	user, ok := auth.ValidateToken(token)
	if !ok {
		t.Fatal("expected successful token validation")
	}
	if user != "admin" {
		t.Fatalf("expected user admin, got %q", user)
	}
}

func TestAuthServiceValidateTokenRejectsTamperedToken(t *testing.T) {
	auth := newAuthService("test-secret", nil)

	token, err := auth.issueToken("admin")
	if err != nil {
		t.Fatalf("issueToken returned error: %v", err)
	}

	tampered := token + "lookatmeimuptonogood"
	_, ok := auth.ValidateToken(tampered)
	if ok {
		t.Fatal("expected tampered token to fail validation")
	}
}
