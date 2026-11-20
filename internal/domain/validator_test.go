package domain

import (
	"strings"
	"testing"
)

func TestValidateNotificationFields(t *testing.T) {
	tests := []struct {
		name      string
		recipient string
		channel   Channel
		content   string
		priority  Priority
		wantErr   bool
	}{
		{
			name:      "valid sms",
			recipient: "+905551234567",
			channel:   ChannelSMS,
			content:   "Your verification code is 123456",
			priority:  PriorityHigh,
		},
		{
			name:      "invalid sms phone",
			recipient: "05551234567",
			channel:   ChannelSMS,
			content:   "Hello",
			priority:  PriorityNormal,
			wantErr:   true,
		},
		{
			name:      "sms exceeds char limit",
			recipient: "+905551234567",
			channel:   ChannelSMS,
			content:   strings.Repeat("a", MaxSMSContentChars+1),
			priority:  PriorityNormal,
			wantErr:   true,
		},
		{
			name:      "valid email",
			recipient: "user@example.com",
			channel:   ChannelEmail,
			content:   "Welcome to Insider One!",
			priority:  PriorityNormal,
		},
		{
			name:      "invalid email missing domain dot",
			recipient: "user@localhost",
			channel:   ChannelEmail,
			content:   "Welcome",
			priority:  PriorityLow,
			wantErr:   true,
		},
		{
			name:      "valid push token",
			recipient: "fcm_token_abcdef1234567890",
			channel:   ChannelPush,
			content:   "Flash sale starts now!",
			priority:  PriorityHigh,
		},
		{
			name:      "invalid push token with spaces",
			recipient: "bad token with spaces",
			channel:   ChannelPush,
			content:   "Hello",
			priority:  PriorityNormal,
			wantErr:   true,
		},
		{
			name:      "invalid channel and priority",
			recipient: "+905551234567",
			channel:   Channel("fax"),
			content:   "Hello",
			priority:  Priority("urgent"),
			wantErr:   true,
		},
		{
			name:      "empty content",
			recipient: "+905551234567",
			channel:   ChannelSMS,
			content:   "   ",
			priority:  PriorityNormal,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateNotificationFields(tc.recipient, tc.channel, tc.content, tc.priority)
			if tc.wantErr && err == nil {
				t.Fatalf("expected validation error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

func TestStatusHelpers(t *testing.T) {
	cancellable := []Status{StatusScheduled, StatusPending, StatusQueued, StatusRetrying}
	for _, s := range cancellable {
		if !s.CanCancel() {
			t.Errorf("expected %s to be cancellable", s)
		}
		if s.IsTerminal() {
			t.Errorf("expected %s not to be terminal", s)
		}
	}

	nonCancellable := []Status{StatusProcessing, StatusDelivered, StatusFailed, StatusCancelled}
	for _, s := range nonCancellable {
		if s.CanCancel() {
			t.Errorf("expected %s not to be cancellable", s)
		}
	}
}
