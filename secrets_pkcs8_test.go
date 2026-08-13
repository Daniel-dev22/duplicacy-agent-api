package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"
)

func TestConvertRSAPrivKeyToPKCS1(t *testing.T) {
	// Generate an RSA key once; use across cases.
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	pkcs1Bytes := x509.MarshalPKCS1PrivateKey(rsaKey)
	pkcs1PEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: pkcs1Bytes})
	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(rsaKey)
	if err != nil {
		t.Fatalf("marshal pkcs8: %v", err)
	}
	pkcs8PEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8Bytes})

	cases := []struct {
		name            string
		input           []byte
		wantBlockType   string
		wantConversion  bool
		wantSameAsInput bool
	}{
		{
			name:            "pkcs1 input → returned unchanged",
			input:           pkcs1PEM,
			wantBlockType:   "RSA PRIVATE KEY",
			wantSameAsInput: true,
		},
		{
			name:           "pkcs8 unencrypted input → converted to pkcs1",
			input:          pkcs8PEM,
			wantBlockType:  "RSA PRIVATE KEY",
			wantConversion: true,
		},
		{
			name:            "garbage input → returned unchanged",
			input:           []byte("not a pem"),
			wantSameAsInput: true,
		},
		{
			name:            "non-private-key PEM → returned unchanged",
			input:           pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("x")}),
			wantSameAsInput: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := convertRSAPrivKeyToPKCS1(tc.input)
			if tc.wantSameAsInput {
				if string(got) != string(tc.input) {
					t.Fatalf("expected unchanged; got diff")
				}
				return
			}
			if !strings.Contains(string(got), tc.wantBlockType) {
				t.Fatalf("expected PEM block type %q in output, got: %s", tc.wantBlockType, got)
			}
			// Round-trip parse to confirm the result is a valid PKCS#1 key.
			block, _ := pem.Decode(got)
			if block == nil {
				t.Fatalf("invalid PEM out")
			}
			if _, err := x509.ParsePKCS1PrivateKey(block.Bytes); err != nil {
				t.Fatalf("pkcs1 parse: %v", err)
			}
		})
	}
}
