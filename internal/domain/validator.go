package domain

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	MaxBatchSize = 1000

	MaxSMSContentChars   = 1600
	MaxEmailContentChars = 10000
	MaxPushContentChars  = 4096

	MinPushTokenLen = 8
	MaxPushTokenLen = 512
)

var e164Regex = regexp.MustCompile(`^\+[1-9]\d{6,14}$`)

// ValidateNotificationFields validates channel, priority, recipient format, and channel content limits.
func ValidateNotificationFields(recipient string, channel Channel, content string, priority Priority) *ValidationError {
	verr := &ValidationError{}

	if !channel.IsValid() {
		verr.Add("channel", "must be one of: sms, email, push")
	}
	if !priority.IsValid() {
		verr.Add("priority", "must be one of: high, normal, low")
	}

	trimmedRecipient := strings.TrimSpace(recipient)
	if trimmedRecipient == "" {
		verr.Add("recipient", "is required")
	} else if channel.IsValid() {
		if msg := validateRecipient(trimmedRecipient, channel); msg != "" {
			verr.Add("recipient", msg)
		}
	}

	if strings.TrimSpace(content) == "" {
		verr.Add("content", "is required")
	} else if channel.IsValid() {
		limit := ContentLimitForChannel(channel)
		if n := utf8.RuneCountInString(content); n > limit {
			verr.Add("content", fmt.Sprintf("exceeds %s limit of %d characters (got %d)", channel, limit, n))
		}
	}

	if verr.HasErrors() {
		return verr
	}
	return nil
}

// ContentLimitForChannel returns the maximum character count allowed for the given channel.
func ContentLimitForChannel(channel Channel) int {
	switch channel {
	case ChannelSMS:
		return MaxSMSContentChars
	case ChannelEmail:
		return MaxEmailContentChars
	case ChannelPush:
		return MaxPushContentChars
	default:
		return MaxSMSContentChars
	}
}

func validateRecipient(recipient string, channel Channel) string {
	switch channel {
	case ChannelSMS:
		if !e164Regex.MatchString(recipient) {
			return "must be a valid E.164 phone number (e.g. +905551234567)"
		}
	case ChannelEmail:
		addr, err := mail.ParseAddress(recipient)
		if err != nil || addr.Address != recipient {
			return "must be a valid email address"
		}
		parts := strings.Split(addr.Address, "@")
		if len(parts) != 2 || !strings.Contains(parts[1], ".") || strings.HasPrefix(parts[1], ".") || strings.HasSuffix(parts[1], ".") {
			return "must be a valid email address with a valid domain"
		}
	case ChannelPush:
		if len(recipient) < MinPushTokenLen || len(recipient) > MaxPushTokenLen {
			return fmt.Sprintf("push device token must be between %d and %d characters", MinPushTokenLen, MaxPushTokenLen)
		}
		for _, r := range recipient {
			if unicode.IsSpace(r) {
				return "push device token must not contain whitespace"
			}
		}
	}
	return ""
}

// ValidateTemplate validates template name, channel, and body length.
func ValidateTemplate(name string, channel Channel, body string) *ValidationError {
	verr := &ValidationError{}
	if strings.TrimSpace(name) == "" {
		verr.Add("name", "is required")
	} else if len(name) > 128 {
		verr.Add("name", "must not exceed 128 characters")
	}
	if !channel.IsValid() {
		verr.Add("channel", "must be one of: sms, email, push")
	}
	if strings.TrimSpace(body) == "" {
		verr.Add("body", "is required")
	} else if channel.IsValid() && utf8.RuneCountInString(body) > ContentLimitForChannel(channel) {
		verr.Add("body", fmt.Sprintf("exceeds %s limit of %d characters", channel, ContentLimitForChannel(channel)))
	}
	if verr.HasErrors() {
		return verr
	}
	return nil
}
