package domain

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

// Content and recipient rules per channel (FR-2.4). They model real provider limits:
//
//   - SMS: E.164 recipient (8–15 digits; a few tiny numbering plans with shorter numbers are not
//     supported); content of at most MaxSMSSegments concatenated segments. A segment
//     carries 160 GSM-7 septets (153 when concatenated), or 70 UCS-2 code units (67) as soon as
//     one character is outside the GSM-7 alphabet.
//   - Email: a single bare address (RFC 5322 addr-spec, at most 254 bytes); body up to 64 KiB.
//   - Push: an opaque device token of printable ASCII; payload up to 4 KiB, the APNs/FCM limit.
const (
	MaxSMSSegments     = 10
	MaxEmailBytes      = 64 << 10
	MaxPushBytes       = 4 << 10
	MaxEmailAddrBytes  = 254
	MinPushTokenLength = 8
	MaxPushTokenLength = 4096
)

var e164 = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)

// ValidateRecipient checks a recipient against the rules of its channel.
func ValidateRecipient(ch Channel, recipient string) error {
	if recipient == "" {
		return invalid("is required")
	}
	switch ch {
	case ChannelSMS:
		if !e164.MatchString(recipient) {
			return invalid("must be an E.164 phone number, e.g. +905551234567")
		}
	case ChannelEmail:
		addr, err := mail.ParseAddress(recipient)
		if err != nil || addr.Address != recipient || addr.Name != "" {
			return invalid("must be a bare email address, e.g. user@example.com")
		}
		if len(recipient) > MaxEmailAddrBytes {
			return invalid(fmt.Sprintf("must be at most %d bytes", MaxEmailAddrBytes))
		}
	case ChannelPush:
		if len(recipient) < MinPushTokenLength || len(recipient) > MaxPushTokenLength {
			return invalid(fmt.Sprintf("must be a device token of %d to %d characters", MinPushTokenLength, MaxPushTokenLength))
		}
		for _, r := range recipient {
			if r > unicode.MaxASCII || !unicode.IsPrint(r) || r == ' ' {
				return invalid("must be a device token of printable ASCII without spaces")
			}
		}
	default:
		return invalid(fmt.Sprintf("cannot be checked for unknown channel %q", ch))
	}
	return nil
}

// ValidateContent checks message content against the limits of its channel.
func ValidateContent(ch Channel, content string) error {
	if !hasVisible(content) {
		return invalid("is required")
	}
	if !utf8.ValidString(content) {
		return invalid("must be valid UTF-8")
	}
	switch ch {
	case ChannelSMS:
		if seg := SMSSegments(content); seg.Count > MaxSMSSegments {
			return invalid(fmt.Sprintf("needs %d SMS segments (%s), at most %d are allowed",
				seg.Count, seg.Encoding, MaxSMSSegments))
		}
	case ChannelEmail:
		if len(content) > MaxEmailBytes {
			return invalid(fmt.Sprintf("must be at most %d bytes", MaxEmailBytes))
		}
	case ChannelPush:
		if len(content) > MaxPushBytes {
			return invalid(fmt.Sprintf("must be at most %d bytes", MaxPushBytes))
		}
	default:
		return invalid(fmt.Sprintf("cannot be checked for unknown channel %q", ch))
	}
	return nil
}

// Segments describes how an SMS text is split for delivery.
type Segments struct {
	Encoding string // "GSM-7" or "UCS-2"
	Count    int
}

// SMSSegments returns the encoding and number of concatenated segments needed for text. It packs
// segments the way carriers do: a concatenated segment holds 153 GSM-7 septets or 67 UCS-2 code
// units, and neither a GSM escape sequence (extension character) nor a UTF-16 surrogate pair is
// split across two segments.
func SMSSegments(text string) Segments {
	gsm := true
	for _, r := range text {
		if !strings.ContainsRune(gsmBasic, r) && !strings.ContainsRune(gsmExtension, r) {
			gsm = false
			break
		}
	}
	if gsm {
		return Segments{Encoding: "GSM-7", Count: pack(text, 160, 153, func(r rune) int {
			if strings.ContainsRune(gsmExtension, r) {
				return 2 // escape + character
			}
			return 1
		})}
	}
	return Segments{Encoding: "UCS-2", Count: pack(text, 70, 67, utf16.RuneLen)}
}

// pack counts segments for units of the given cost per rune: one segment if everything fits in
// single, otherwise multi-unit segments filled greedily without splitting a rune.
func pack(text string, single, multi int, cost func(rune) int) int {
	total := 0
	for _, r := range text {
		total += cost(r)
	}
	if total <= single {
		return 1
	}
	count, used := 1, 0
	for _, r := range text {
		c := cost(r)
		if used+c > multi {
			count, used = count+1, 0
		}
		used += c
	}
	return count
}

// GSM 03.38 default alphabet and its extension table (characters sent as escape + char).
const (
	gsmBasic = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?" +
		"¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"
	gsmExtension = "^{}\\[~]|€\f"
)

func invalid(msg string) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, msg)
}

// hasVisible reports whether s contains a character that renders, so content made only of
// spaces or zero-width characters counts as empty.
func hasVisible(s string) bool {
	for _, r := range s {
		if unicode.IsGraphic(r) && !unicode.IsSpace(r) {
			return true
		}
	}
	return false
}
