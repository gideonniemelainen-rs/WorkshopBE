package main

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
