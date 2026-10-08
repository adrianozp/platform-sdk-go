package queue

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

const SignatureMaxAge = 5 * time.Minute

var ErrBadSignature = errors.New("queue: delivery signature is missing, malformed, expired or wrong")

func Sign(key string, at time.Time, body []byte) string {
	stamp := strconv.FormatInt(at.Unix(), 10)
	return "t=" + stamp + ",v1=" + mac(key, stamp, body)
}

func Verify(header, key string, body []byte, now time.Time) error {
	stampPart, macPart, ok := strings.Cut(header, ",")
	stamp, okStamp := strings.CutPrefix(stampPart, "t=")
	sent, okMac := strings.CutPrefix(macPart, "v1=")
	signedAt, err := strconv.ParseInt(stamp, 10, 64)
	if !ok || !okStamp || !okMac || err != nil || key == "" {
		return ErrBadSignature
	}
	if now.Sub(time.Unix(signedAt, 0)).Abs() > SignatureMaxAge {
		return ErrBadSignature
	}
	if !hmac.Equal([]byte(mac(key, stamp, body)), []byte(sent)) {
		return ErrBadSignature
	}
	return nil
}

func mac(key, stamp string, body []byte) string {
	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte(stamp + "."))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}
