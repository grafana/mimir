package telebot

import "time"

// VoiceChatStarted represents a service message about a voice chat
// started in the chat.
type VoiceChatStarted struct{}

// VoiceChatEnded represents a service message about a voice chat
// ended in the chat.
type VoiceChatEnded struct {
	Duration int `json:"duration"` // in seconds
}

// VoiceChatParticipants represents a service message about new
// members invited to a voice chat
type VoiceChatParticipants struct {
	Users []User `json:"users"`
}

// VoiceChatScheduled represents a service message about a voice chat scheduled in the chat.
type VoiceChatScheduled struct {
	Unixtime int64 `json:"start_date"`
}

// StartsAt returns the point when the voice chat is supposed to be started by a chat administrator.
func (v *VoiceChatScheduled) StartsAt() time.Time {
	return time.Unix(v.Unixtime, 0)
}
