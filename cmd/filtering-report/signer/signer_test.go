// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package signer

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/offchainlabs/nitro/cmd/filtering-report/signer/signertest"
)

const testSAN = "https://test-webhook-signer.internal"

func TestSigner_RoundTripVerifiedByVerifier(t *testing.T) {
	pemPath, caPath := signertest.SigningFixture(t, signertest.DefaultLeafOptions(testSAN))

	s, err := NewSigner(&Config{PEMFile: pemPath})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	v, err := NewVerifier(&VerifierConfig{
		CARootPEMFile: caPath,
		ExpectedSAN:   testSAN,
	})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	body := []byte(`{"event":"transfer","amount":"1.5"}`)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	if err := s.SignHTTPRequest(req, body, time.Now()); err != nil {
		t.Fatalf("SignHTTPRequest: %v", err)
	}
	if err := v.VerifyHTTPRequest(req, body); err != nil {
		t.Fatalf("VerifyHTTPRequest: %v", err)
	}
}

func TestSigner_ReloadPicksUpNewCert(t *testing.T) {
	pki := signertest.NewPKI(t)
	priv1, leafDER1 := pki.IssueLeaf(t, signertest.DefaultLeafOptions(testSAN))
	dir := t.TempDir()
	pemPath := signertest.WriteCombinedPEM(t, dir, priv1, leafDER1)

	s, err := NewSigner(&Config{PEMFile: pemPath})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	first := s.creds.Load().leafCert.Raw

	priv2, leafDER2 := pki.IssueLeaf(t, signertest.DefaultLeafOptions(testSAN))
	keyPEM2, certPEM2 := signertest.EncodePEMBundle(t, priv2, leafDER2)
	if err := os.WriteFile(pemPath, append(keyPEM2, certPEM2...), 0o600); err != nil {
		t.Fatalf("rewrite PEM: %v", err)
	}
	if err := s.reloadConfig(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if bytes.Equal(first, s.creds.Load().leafCert.Raw) {
		t.Fatal("expected leafDER to change after reload")
	}
}

func TestSigner_ReloadKeepsOldOnParseError(t *testing.T) {
	pki := signertest.NewPKI(t)
	priv, leafDER := pki.IssueLeaf(t, signertest.DefaultLeafOptions(testSAN))
	dir := t.TempDir()
	pemPath := signertest.WriteCombinedPEM(t, dir, priv, leafDER)

	s, err := NewSigner(&Config{PEMFile: pemPath})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	original := s.creds.Load()

	if err := os.WriteFile(pemPath, []byte("not a pem"), 0o600); err != nil {
		t.Fatalf("rewrite PEM: %v", err)
	}
	if err := s.reloadConfig(); err == nil {
		t.Fatal("expected reload error on garbage PEM")
	}
	if s.creds.Load() != original {
		t.Fatal("expected credentials to be retained after parse failure")
	}
}
