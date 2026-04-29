// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package signer

import (
	"slices"
	"testing"

	"github.com/offchainlabs/nitro/cmd/filtering-report/signer/signertest"
)

func TestParseCombinedPEM_RejectsMismatchedKeyAndCert(t *testing.T) {
	pki := signertest.NewPKI(t)
	priv1, _, _ := pki.IssueLeaf(t, signertest.DefaultLeafOptions(testSAN))
	_, _, leafDER2 := pki.IssueLeaf(t, signertest.DefaultLeafOptions(testSAN))

	keyPEM, certPEM := signertest.EncodePEMBundle(t, priv1, leafDER2)
	bundle := slices.Concat(keyPEM, certPEM)

	_, err := parseCombinedPEM(bundle)
	assertVerifyError(t, err, "private key does not match leaf certificate public key")
}

func TestParseCombinedPEM_RejectsKeyOnly(t *testing.T) {
	pki := signertest.NewPKI(t)
	priv, _, leafDER := pki.IssueLeaf(t, signertest.DefaultLeafOptions(testSAN))
	keyPEM, _ := signertest.EncodePEMBundle(t, priv, leafDER)

	_, err := parseCombinedPEM(keyPEM)
	assertVerifyError(t, err, "no CERTIFICATE block found in PEM")
}

func TestParseCombinedPEM_RejectsCertOnly(t *testing.T) {
	pki := signertest.NewPKI(t)
	priv, _, leafDER := pki.IssueLeaf(t, signertest.DefaultLeafOptions(testSAN))
	_, certPEM := signertest.EncodePEMBundle(t, priv, leafDER)

	_, err := parseCombinedPEM(certPEM)
	assertVerifyError(t, err, "no PRIVATE KEY block found in PEM")
}

func TestParseCombinedPEM_RejectsDuplicatePrivateKey(t *testing.T) {
	pki := signertest.NewPKI(t)
	priv, _, leafDER := pki.IssueLeaf(t, signertest.DefaultLeafOptions(testSAN))
	keyPEM, certPEM := signertest.EncodePEMBundle(t, priv, leafDER)
	bundle := slices.Concat(keyPEM, keyPEM, certPEM)

	_, err := parseCombinedPEM(bundle)
	assertVerifyError(t, err, "PEM contains more than one PRIVATE KEY block")
}

func TestParseCombinedPEM_RejectsCAAsLeaf(t *testing.T) {
	pki := signertest.NewPKI(t)
	keyPEM, certPEM := signertest.EncodePEMBundle(t, pki.CAPriv, pki.CACertDER)
	bundle := slices.Concat(keyPEM, certPEM)

	_, err := parseCombinedPEM(bundle)
	assertVerifyError(t, err, "first certificate in PEM is a CA, expected leaf")
}

func TestParseCombinedPEM_RejectsUnsupportedBlockType(t *testing.T) {
	bundle := []byte("-----BEGIN EC PRIVATE KEY-----\nQUJD\n-----END EC PRIVATE KEY-----\n")
	_, err := parseCombinedPEM(bundle)
	assertVerifyError(t, err, "unsupported PEM block type")
}
