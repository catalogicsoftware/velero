package bundlecrypt

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/iotest"
)

// The golden values were produced by an independent Python implementation of
// the format (HKDF and AES-GCM from the cryptography package).
const (
	goldenPassword    = "cloudcasa-golden-password"
	goldenBundleKey   = "47737d03c4f9cb6469c76d14baa626e8b42c4dc489daf09a173205af9c26fe92"
	goldenFingerprint = "518f00aeb527ba1b"
	goldenSmallPlain  = "CloudCasa bundle v1"
	goldenSmallCipher = "4343564201100000000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
		"518f00aeb527ba1b433e238bc023f02fc0e67c21981a68c427d074614472198c5e6a5883c54e0024ab400b"
)

func goldenKey(t *testing.T) []byte {
	t.Helper()
	key, err := DeriveBundleKey(goldenPassword)
	if err != nil {
		t.Fatalf("DeriveBundleKey: %v", err)
	}
	return key
}

func goldenSalt() []byte {
	salt := make([]byte, saltSize)
	for i := range salt {
		salt[i] = byte(i)
	}
	return salt
}

func pattern(size int, mul, add byte) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)*mul + add
	}
	return data
}

func encryptWithSalt(t *testing.T, plain, key, salt []byte) []byte {
	t.Helper()
	enc, err := newEncrypter(bytes.NewReader(plain), key, salt)
	if err != nil {
		t.Fatalf("newEncrypter: %v", err)
	}
	sealed, err := io.ReadAll(enc)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	return sealed
}

func encrypt(t *testing.T, plain, key []byte) []byte {
	t.Helper()
	enc, err := Encrypt(bytes.NewReader(plain), key)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	sealed, err := io.ReadAll(enc)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	return sealed
}

func decrypt(sealed, key []byte) ([]byte, error) {
	dec, err := Decrypt(bytes.NewReader(sealed), key)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(dec)
}

func TestGoldenKeyAndFingerprint(t *testing.T) {
	key := goldenKey(t)
	if hex.EncodeToString(key) != goldenBundleKey {
		t.Fatalf("bundle key %x, want %s", key, goldenBundleKey)
	}
	if hex.EncodeToString(Fingerprint(key)) != goldenFingerprint {
		t.Fatalf("fingerprint %x, want %s", Fingerprint(key), goldenFingerprint)
	}
	if _, err := DeriveBundleKey(""); err == nil {
		t.Fatal("an empty password must not make a key")
	}
}

func TestGoldenCiphertexts(t *testing.T) {
	key := goldenKey(t)
	small := encryptWithSalt(t, []byte(goldenSmallPlain), key, goldenSalt())
	if hex.EncodeToString(small) != goldenSmallCipher {
		t.Fatalf("small ciphertext\n got %x\nwant %s", small, goldenSmallCipher)
	}
	want, _ := hex.DecodeString(goldenSmallCipher)
	plain, err := decrypt(want, key)
	if err != nil || string(plain) != goldenSmallPlain {
		t.Fatalf("golden small decrypts to %q, %v", plain, err)
	}

	for _, tc := range []struct {
		name   string
		plain  []byte
		size   int
		sha256 string
	}{
		{"empty", nil, 64, "5d94c4c0f1de1a40e2292b4dd4f46b49ea9453e60129f92db340b8eb9352842f"},
		{"exact chunk", pattern(chunkSize, 13, 1), 65600, "3326b0785efa1466265782e47c52e7c7f704e657527111f8f4fca588ce34d9d5"},
		{"three chunks", pattern(2*chunkSize+7, 31, 7), 131175, "c6285e6ea7cda335867f6efdee4b36cf3e50ffcf758973dc53eaf32914cdbd79"},
	} {
		sealed := encryptWithSalt(t, tc.plain, key, goldenSalt())
		sum := sha256.Sum256(sealed)
		if len(sealed) != tc.size || hex.EncodeToString(sum[:]) != tc.sha256 {
			t.Errorf("%s: %d bytes, sha256 %x; want %d bytes, %s", tc.name, len(sealed), sum, tc.size, tc.sha256)
		}
		plain, err := decrypt(sealed, key)
		if err != nil || !bytes.Equal(plain, tc.plain) {
			t.Errorf("%s: round trip failed: %v", tc.name, err)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	key := goldenKey(t)
	for _, size := range []int{0, 1, chunkSize - 1, chunkSize, chunkSize + 1, 3 * chunkSize, 10<<20 + 3} {
		plain := pattern(size, 7, 3)
		sealed := encrypt(t, plain, key)
		if want := headerSize + size + tagSize*(max(1, (size+chunkSize-1)/chunkSize)); len(sealed) != want {
			t.Errorf("size %d: %d sealed bytes, want %d", size, len(sealed), want)
		}
		got, err := decrypt(sealed, key)
		if err != nil || !bytes.Equal(got, plain) {
			t.Errorf("size %d: round trip failed: %v", size, err)
		}
	}
}

func TestEverySaltDiffers(t *testing.T) {
	key := goldenKey(t)
	first := encrypt(t, []byte("same"), key)
	second := encrypt(t, []byte("same"), key)
	if bytes.Equal(first, second) {
		t.Fatal("two encryptions of the same file must differ")
	}
}

func TestOneByteReads(t *testing.T) {
	key := goldenKey(t)
	plain := pattern(chunkSize+100, 5, 9)
	enc, err := Encrypt(iotest.OneByteReader(bytes.NewReader(plain)), key)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	sealed, err := io.ReadAll(iotest.OneByteReader(enc))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	dec, err := Decrypt(iotest.OneByteReader(bytes.NewReader(sealed)), key)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	got, err := io.ReadAll(iotest.OneByteReader(dec))
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("one-byte reads: %v", err)
	}
}

func TestTamperingIsDetected(t *testing.T) {
	key := goldenKey(t)
	sealed := encryptWithSalt(t, pattern(2*chunkSize+7, 31, 7), key, goldenSalt())
	for _, at := range []int{5, saltOffset + 3, fingerprintOffset + 1, headerSize + 10, headerSize + chunkSize + tagSize - 1, len(sealed) - 1} {
		changed := bytes.Clone(sealed)
		changed[at] ^= 0x01
		if _, err := decrypt(changed, key); err == nil {
			t.Errorf("a flipped bit at byte %d was not detected", at)
		}
	}
}

func TestTruncationIsDetected(t *testing.T) {
	key := goldenKey(t)
	sealed := encryptWithSalt(t, pattern(2*chunkSize+7, 31, 7), key, goldenSalt())
	firstChunkEnd := headerSize + chunkSize + tagSize
	for _, cut := range []int{0, 10, headerSize, headerSize + 5, firstChunkEnd, firstChunkEnd + 1, len(sealed) - 1} {
		if _, err := decrypt(sealed[:cut], key); !errors.Is(err, ErrCorrupt) {
			t.Errorf("cut at %d: got %v, want ErrCorrupt", cut, err)
		}
	}
	if _, err := decrypt(append(bytes.Clone(sealed), 0), key); !errors.Is(err, ErrCorrupt) {
		t.Errorf("an appended byte: got %v, want ErrCorrupt", err)
	}
}

func TestReorderedChunksAreDetected(t *testing.T) {
	key := goldenKey(t)
	sealed := encryptWithSalt(t, pattern(3*chunkSize+1, 3, 1), key, goldenSalt())
	sealedChunk := chunkSize + tagSize
	first := sealed[headerSize : headerSize+sealedChunk]
	second := sealed[headerSize+sealedChunk : headerSize+2*sealedChunk]

	swapped := bytes.Clone(sealed)
	copy(swapped[headerSize:], second)
	copy(swapped[headerSize+sealedChunk:], first)
	if _, err := decrypt(swapped, key); !errors.Is(err, ErrCorrupt) {
		t.Errorf("swapped chunks: got %v, want ErrCorrupt", err)
	}

	duplicated := bytes.Clone(sealed)
	copy(duplicated[headerSize+sealedChunk:], first)
	if _, err := decrypt(duplicated, key); !errors.Is(err, ErrCorrupt) {
		t.Errorf("duplicated chunk: got %v, want ErrCorrupt", err)
	}
}

func TestWrongKeyIsNamed(t *testing.T) {
	sealed := encrypt(t, []byte("secret"), goldenKey(t))
	other, err := DeriveBundleKey("another-password")
	if err != nil {
		t.Fatalf("DeriveBundleKey: %v", err)
	}
	if _, err := decrypt(sealed, other); !errors.Is(err, ErrWrongKey) {
		t.Fatalf("got %v, want ErrWrongKey", err)
	}
	if _, err := Decrypt(bytes.NewReader(sealed), []byte("short")); err == nil {
		t.Fatal("a short key must be refused")
	}
}

func TestOpen(t *testing.T) {
	key := goldenKey(t)
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	w.Write([]byte("resources"))
	w.Close()
	plainFiles := map[string][]byte{
		"gzip":  gz.Bytes(),
		"json":  []byte(`{"kind":"Backup"}`),
		"empty": nil,
		"short": []byte("CC"),
	}

	for name, plain := range plainFiles {
		r, err := Open(bytes.NewReader(plain), key, false)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if got, err := io.ReadAll(r); err != nil || !bytes.Equal(got, plain) {
			t.Errorf("%s: passed through as %q, %v", name, got, err)
		}
		if _, err := Open(bytes.NewReader(plain), key, true); !errors.Is(err, ErrNotEncrypted) {
			t.Errorf("%s in required mode: got %v, want ErrNotEncrypted", name, err)
		}
		if _, err := Open(bytes.NewReader(plain), nil, false); err != nil {
			t.Errorf("%s without a key: %v", name, err)
		}
	}

	sealed := encrypt(t, gz.Bytes(), key)
	for _, required := range []bool{false, true} {
		r, err := Open(bytes.NewReader(sealed), key, required)
		if err != nil {
			t.Fatalf("required=%v: %v", required, err)
		}
		if got, err := io.ReadAll(r); err != nil || !bytes.Equal(got, gz.Bytes()) {
			t.Fatalf("required=%v: decrypted to %q, %v", required, got, err)
		}
	}
	if _, err := Open(bytes.NewReader(sealed), nil, false); !errors.Is(err, ErrNoKey) {
		t.Fatalf("an encrypted file without a key: got %v, want ErrNoKey", err)
	}
	if !IsEncrypted(sealed) || IsEncrypted(gz.Bytes()) {
		t.Fatal("IsEncrypted is wrong")
	}
}

func TestDecryptErrorsAreRecognised(t *testing.T) {
	key := goldenKey(t)
	newer := encrypt(t, []byte("x"), key)
	newer[4] = version1 + 1
	if _, err := decrypt(newer, key); !errors.Is(err, ErrUnknownVersion) || !IsDecryptError(err) {
		t.Fatalf("a newer format: got %v", err)
	}
	if !IsDecryptError(fmt.Errorf("reading the tarball: %w", ErrCorrupt)) {
		t.Fatal("a wrapped ErrCorrupt must be recognised")
	}
	if IsDecryptError(nil) || IsDecryptError(io.ErrUnexpectedEOF) || IsDecryptError(errors.New("not found")) {
		t.Fatal("errors from fetching a file are not decrypt errors")
	}
}

func TestSourceErrorsPassThrough(t *testing.T) {
	key := goldenKey(t)
	broken := errors.New("network down")
	enc, err := Encrypt(iotest.ErrReader(broken), key)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := io.ReadAll(enc); !errors.Is(err, broken) {
		t.Fatalf("encrypt: got %v, want the source error", err)
	}

	sealed := encrypt(t, pattern(chunkSize+5, 1, 1), key)
	src := io.MultiReader(bytes.NewReader(sealed[:headerSize+100]), iotest.ErrReader(broken))
	dec, err := Decrypt(src, key)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if _, err := io.ReadAll(dec); !errors.Is(err, broken) {
		t.Fatalf("decrypt: got %v, want the source error", err)
	}
}
