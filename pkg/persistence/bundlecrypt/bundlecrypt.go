// Package bundlecrypt encrypts Velero bundle files and reads them back.
//
// This file is kept identical in amdslib/bundlecrypt and in the Velero fork at
// pkg/persistence/bundlecrypt. The golden test pins the format in both.
package bundlecrypt

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// KeySize is the length of a bundle key.
	KeySize = 32

	// FormatV1 names this format wherever it is recorded.
	FormatV1 = "V1"

	saltOffset        = 8
	saltSize          = 32
	fingerprintOffset = saltOffset + saltSize
	fingerprintSize   = 8
	headerSize        = fingerprintOffset + fingerprintSize
	version1          = 0x01
	chunkSizeLog2     = 16
	chunkSize         = 1 << chunkSizeLog2
	tagSize           = 16
	nonceSize         = 12

	bundleKeyInfo   = "cloudcasa/velero-bundle/v1"
	fileKeyInfo     = "cloudcasa/velero-bundle/file/v1"
	fingerprintInfo = "cloudcasa/velero-bundle/fingerprint/v1"
)

var magic = []byte("CCVB")

var (
	// ErrNotEncrypted is returned for a plain file where only an encrypted one is accepted.
	ErrNotEncrypted = errors.New("bundlecrypt: the file is not encrypted")

	// ErrNoKey is returned for an encrypted file when no key was given.
	ErrNoKey = errors.New("bundlecrypt: the file is encrypted but no key was given")

	// ErrWrongKey is returned when the file was encrypted with another key.
	ErrWrongKey = errors.New("bundlecrypt: the file was encrypted with a different key")

	// ErrCorrupt is returned when an encrypted file was changed or cut short.
	ErrCorrupt = errors.New("bundlecrypt: the encrypted file is damaged or was changed")

	// ErrUnknownVersion is returned for a file in a newer format.
	ErrUnknownVersion = errors.New("bundlecrypt: unknown format version")
)

// IsDecryptError reports whether err came from reading an encrypted file, as
// opposed to fetching it.
func IsDecryptError(err error) bool {
	for _, known := range []error{ErrNotEncrypted, ErrNoKey, ErrWrongKey, ErrCorrupt, ErrUnknownVersion} {
		if errors.Is(err, known) {
			return true
		}
	}
	return false
}

// DeriveBundleKey turns a store's repository password into its bundle key.
func DeriveBundleKey(password string) ([]byte, error) {
	if password == "" {
		return nil, errors.New("bundlecrypt: the repository password is empty")
	}
	return hkdf.Key(sha256.New, []byte(password), nil, bundleKeyInfo, KeySize)
}

// Fingerprint identifies a bundle key without revealing it.
func Fingerprint(bundleKey []byte) []byte {
	mac := hmac.New(sha256.New, bundleKey)
	mac.Write([]byte(fingerprintInfo))
	return mac.Sum(nil)[:fingerprintSize]
}

// IsEncrypted reports whether data that starts with prefix is in this format.
func IsEncrypted(prefix []byte) bool {
	return bytes.HasPrefix(prefix, magic)
}

// Encrypt returns a reader of src encrypted with bundleKey.
func Encrypt(src io.Reader, bundleKey []byte) (io.Reader, error) {
	salt := make([]byte, saltSize)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("bundlecrypt: could not make a random salt: %w", err)
	}
	return newEncrypter(src, bundleKey, salt)
}

// Decrypt returns a reader of the plaintext of src, which must be encrypted.
func Decrypt(src io.Reader, bundleKey []byte) (io.Reader, error) {
	if err := checkKey(bundleKey); err != nil {
		return nil, err
	}
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(src, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrCorrupt
		}
		return nil, err
	}
	if !IsEncrypted(header) {
		return nil, ErrNotEncrypted
	}
	if header[4] != version1 {
		return nil, fmt.Errorf("%w %d", ErrUnknownVersion, header[4])
	}
	if header[5] != chunkSizeLog2 || header[6] != 0 || header[7] != 0 {
		return nil, ErrCorrupt
	}
	if subtle.ConstantTimeCompare(header[fingerprintOffset:], Fingerprint(bundleKey)) != 1 {
		return nil, ErrWrongKey
	}
	aead, err := fileAEAD(bundleKey, header[saltOffset:fingerprintOffset])
	if err != nil {
		return nil, err
	}
	return &decrypter{src: src, aead: aead, header: header, in: make([]byte, chunkSize+tagSize+1)}, nil
}

// Open returns a reader of the plaintext of src. An encrypted file is
// decrypted; any other file passes through unless required is set.
func Open(src io.Reader, bundleKey []byte, required bool) (io.Reader, error) {
	buffered := bufio.NewReader(src)
	prefix, err := buffered.Peek(len(magic))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if !IsEncrypted(prefix) {
		if required {
			return nil, ErrNotEncrypted
		}
		return buffered, nil
	}
	if len(bundleKey) == 0 {
		return nil, ErrNoKey
	}
	return Decrypt(buffered, bundleKey)
}

func checkKey(bundleKey []byte) error {
	if len(bundleKey) != KeySize {
		return fmt.Errorf("bundlecrypt: a bundle key is %d bytes, not %d", KeySize, len(bundleKey))
	}
	return nil
}

func fileAEAD(bundleKey, salt []byte) (cipher.AEAD, error) {
	fileKey, err := hkdf.Key(sha256.New, bundleKey, salt, fileKeyInfo, KeySize)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(fileKey)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// nonce is the 11-byte big-endian chunk index followed by the last-chunk flag.
func nonce(index uint64, last bool) []byte {
	n := make([]byte, nonceSize)
	binary.BigEndian.PutUint64(n[nonceSize-9:nonceSize-1], index)
	if last {
		n[nonceSize-1] = 1
	}
	return n
}

// readChunk fills buf from carry onwards. The chunk is the last one when the
// source ends before buf is full; buf is one byte longer than a whole chunk.
func readChunk(src io.Reader, buf []byte, carry int) (int, bool, error) {
	n, err := io.ReadFull(src, buf[carry:])
	n += carry
	switch {
	case err == nil:
		return n, false, nil
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return n, true, nil
	default:
		return 0, false, err
	}
}

type encrypter struct {
	src    io.Reader
	aead   cipher.AEAD
	header []byte
	index  uint64
	in     []byte
	carry  int
	sealed []byte
	out    []byte
	done   bool
	err    error
}

func newEncrypter(src io.Reader, bundleKey, salt []byte) (*encrypter, error) {
	if err := checkKey(bundleKey); err != nil {
		return nil, err
	}
	if len(salt) != saltSize {
		return nil, fmt.Errorf("bundlecrypt: a salt is %d bytes, not %d", saltSize, len(salt))
	}
	header := make([]byte, headerSize)
	copy(header, magic)
	header[4] = version1
	header[5] = chunkSizeLog2
	copy(header[saltOffset:], salt)
	copy(header[fingerprintOffset:], Fingerprint(bundleKey))
	aead, err := fileAEAD(bundleKey, salt)
	if err != nil {
		return nil, err
	}
	return &encrypter{src: src, aead: aead, header: header, in: make([]byte, chunkSize+1), out: header}, nil
}

func (e *encrypter) Read(p []byte) (int, error) {
	for len(e.out) == 0 {
		if e.err != nil {
			return 0, e.err
		}
		if e.done {
			return 0, io.EOF
		}
		e.sealNext()
	}
	n := copy(p, e.out)
	e.out = e.out[n:]
	return n, nil
}

func (e *encrypter) sealNext() {
	n, last, err := readChunk(e.src, e.in, e.carry)
	if err != nil {
		e.err = err
		return
	}
	size := n
	if !last {
		size = chunkSize
	}
	e.sealed = e.aead.Seal(e.sealed[:0], nonce(e.index, last), e.in[:size], e.header)
	e.out = e.sealed
	e.index++
	if last {
		e.done = true
		return
	}
	e.in[0] = e.in[chunkSize]
	e.carry = 1
}

type decrypter struct {
	src    io.Reader
	aead   cipher.AEAD
	header []byte
	index  uint64
	in     []byte
	carry  int
	plain  []byte
	out    []byte
	done   bool
	err    error
}

func (d *decrypter) Read(p []byte) (int, error) {
	for len(d.out) == 0 {
		if d.err != nil {
			return 0, d.err
		}
		if d.done {
			return 0, io.EOF
		}
		d.openNext()
	}
	n := copy(p, d.out)
	d.out = d.out[n:]
	return n, nil
}

func (d *decrypter) openNext() {
	n, last, err := readChunk(d.src, d.in, d.carry)
	if err != nil {
		d.err = err
		return
	}
	size := n
	if !last {
		size = chunkSize + tagSize
	}
	if size < tagSize {
		d.err = ErrCorrupt
		return
	}
	plain, err := d.aead.Open(d.plain[:0], nonce(d.index, last), d.in[:size], d.header)
	if err != nil {
		d.err = ErrCorrupt
		return
	}
	d.plain = plain
	d.out = plain
	d.index++
	if last {
		d.done = true
		return
	}
	d.in[0] = d.in[chunkSize+tagSize]
	d.carry = 1
}
