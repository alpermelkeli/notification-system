package domain

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateRecipient(t *testing.T) {
	tests := []struct {
		ch        Channel
		recipient string
		ok        bool
	}{
		{ChannelSMS, "+905551234567", true},
		{ChannelSMS, "+14155552671", true},
		{ChannelSMS, "905551234567", false},   // no +
		{ChannelSMS, "+0555123456", false},    // country code cannot start with 0
		{ChannelSMS, "+90 555 123 45", false}, // spaces
		{ChannelSMS, "+1234567", false},       // too short
		{ChannelSMS, "+1234567890123456", false},
		{ChannelEmail, "user@example.com", true},
		{ChannelEmail, "first.last+tag@sub.example.co", true},
		{ChannelEmail, "User <user@example.com>", false}, // display name
		{ChannelEmail, "not-an-email", false},
		{ChannelEmail, strings.Repeat("a", 250) + "@x.io", false},
		{ChannelPush, "fcm:APA91bHun4MxP5egoKMwt2KZFBaFUH", true},
		{ChannelPush, "short", false},
		{ChannelPush, "token with spaces", false},
		{ChannelPush, "tökenwithumlaut", false},
		{ChannelSMS, "", false},
		{"fax", "+905551234567", false},
	}
	for _, tt := range tests {
		t.Run(string(tt.ch)+" "+tt.recipient, func(t *testing.T) {
			err := ValidateRecipient(tt.ch, tt.recipient)
			if tt.ok && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.ok && !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestValidateContent(t *testing.T) {
	tests := []struct {
		name    string
		ch      Channel
		content string
		ok      bool
	}{
		{"sms short", ChannelSMS, "Your code is 1234", true},
		{"sms 10 GSM segments", ChannelSMS, strings.Repeat("a", 1530), true},
		{"sms 11 GSM segments", ChannelSMS, strings.Repeat("a", 1531), false},
		{"sms 10 UCS-2 segments", ChannelSMS, strings.Repeat("ş", 670), true},
		{"sms 11 UCS-2 segments", ChannelSMS, strings.Repeat("ş", 671), false},
		{"blank", ChannelSMS, "   \n", false},
		{"only zero-width characters", ChannelPush, "\u200b\u200d\ufeff", false},
		{"invalid utf-8", ChannelPush, "\xff\xfe", false},
		{"email at limit", ChannelEmail, strings.Repeat("x", MaxEmailBytes), true},
		{"email too long", ChannelEmail, strings.Repeat("x", MaxEmailBytes+1), false},
		{"push at limit", ChannelPush, strings.Repeat("x", MaxPushBytes), true},
		{"push too long", ChannelPush, strings.Repeat("x", MaxPushBytes+1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateContent(tt.ch, tt.content)
			if tt.ok && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.ok && !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestSMSSegments(t *testing.T) {
	tests := []struct {
		text string
		want Segments
	}{
		{strings.Repeat("a", 160), Segments{"GSM-7", 1}},
		{strings.Repeat("a", 161), Segments{"GSM-7", 2}},
		{strings.Repeat("a", 306), Segments{"GSM-7", 2}},
		{strings.Repeat("a", 307), Segments{"GSM-7", 3}},
		{strings.Repeat("€", 80), Segments{"GSM-7", 1}}, // extension chars take two septets
		{strings.Repeat("€", 81), Segments{"GSM-7", 2}},
		{"Merhaba, kodunuz: 1234", Segments{"GSM-7", 1}},
		{"Şifreniz: 1234", Segments{"UCS-2", 1}}, // Ş is not in GSM-7
		{strings.Repeat("ı", 70), Segments{"UCS-2", 1}},
		{strings.Repeat("ı", 71), Segments{"UCS-2", 2}},
		{strings.Repeat("😀", 35), Segments{"UCS-2", 1}}, // surrogate pairs count twice
		{strings.Repeat("😀", 36), Segments{"UCS-2", 2}},
		// A concatenated segment cannot split an escape sequence: 152 basic chars + "€" need
		// 154 septets, which does not fit in 153.
		{strings.Repeat("a", 152) + "€" + strings.Repeat("a", 10), Segments{"GSM-7", 2}},
		{strings.Repeat("a", 152) + "€" + strings.Repeat("a", 152), Segments{"GSM-7", 3}},
		// Nor a surrogate pair: 66 units + 😀 (2 units) spills into the next segment.
		{strings.Repeat("ş", 66) + "😀" + strings.Repeat("ş", 66), Segments{"UCS-2", 3}},
		{strings.Repeat("ş", 65) + "😀" + strings.Repeat("ş", 67), Segments{"UCS-2", 2}},
	}
	for _, tt := range tests {
		if got := SMSSegments(tt.text); got != tt.want {
			t.Errorf("SMSSegments(%.20q…) = %+v, want %+v", tt.text, got, tt.want)
		}
	}
}
