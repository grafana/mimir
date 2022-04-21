package telebot

import (
	"encoding/json"
	"strconv"
	"time"
)

// User object represents a Telegram user, bot.
type User struct {
	ID int64 `json:"id"`

	FirstName    string `json:"first_name"`
	LastName     string `json:"last_name"`
	Username     string `json:"username"`
	LanguageCode string `json:"language_code"`
	IsBot        bool   `json:"is_bot"`

	// Returns only in getMe
	CanJoinGroups   bool `json:"can_join_groups"`
	CanReadMessages bool `json:"can_read_all_group_messages"`
	SupportsInline  bool `json:"supports_inline_queries"`
}

// Recipient returns user ID (see Recipient interface).
func (u *User) Recipient() string {
	return strconv.FormatInt(u.ID, 10)
}

// Chat object represents a Telegram user, bot, group or a channel.
type Chat struct {
	ID int64 `json:"id"`

	// See ChatType and consts.
	Type ChatType `json:"type"`

	// Won't be there for ChatPrivate.
	Title string `json:"title"`

	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Username  string `json:"username"`

	// Still shows whether the user is a member
	// of the chat at the moment of the request.
	Still bool `json:"is_member,omitempty"`

	// Returns only in getChat
	Bio              string        `json:"bio,omitempty"`
	Photo            *ChatPhoto    `json:"photo,omitempty"`
	Description      string        `json:"description,omitempty"`
	InviteLink       string        `json:"invite_link,omitempty"`
	PinnedMessage    *Message      `json:"pinned_message,omitempty"`
	Permissions      *Rights       `json:"permissions,omitempty"`
	SlowMode         int           `json:"slow_mode_delay,omitempty"`
	StickerSet       string        `json:"sticker_set_name,omitempty"`
	CanSetStickerSet bool          `json:"can_set_sticker_set,omitempty"`
	LinkedChatID     int64         `json:"linked_chat_id,omitempty"`
	ChatLocation     *ChatLocation `json:"location,omitempty"`
	Private          bool          `json:"has_private_forwards,omitempty"`
	Protected        bool          `json:"has_protected_content,omitempty"`
}

type ChatLocation struct {
	Location Location `json:"location,omitempty"`
	Address  string   `json:"address,omitempty"`
}

// ChatPhoto object represents a chat photo.
type ChatPhoto struct {
	// File identifiers of small (160x160) chat photo
	SmallFileID   string `json:"small_file_id"`
	SmallUniqueID string `json:"small_file_unique_id"`

	// File identifiers of big (640x640) chat photo
	BigFileID   string `json:"big_file_id"`
	BigUniqueID string `json:"big_file_unique_id"`
}

// Recipient returns chat ID (see Recipient interface).
func (c *Chat) Recipient() string {
	return strconv.FormatInt(c.ID, 10)
}

// ChatMember object represents information about a single chat member.
type ChatMember struct {
	Rights

	User      *User        `json:"user"`
	Role      MemberStatus `json:"status"`
	Title     string       `json:"custom_title"`
	Anonymous bool         `json:"is_anonymous"`

	// Date when restrictions will be lifted for the user, unix time.
	//
	// If user is restricted for more than 366 days or less than
	// 30 seconds from the current time, they are considered to be
	// restricted forever.
	//
	// Use tele.Forever().
	//
	RestrictedUntil int64 `json:"until_date,omitempty"`
}

// ChatID represents a chat or an user integer ID, which can be used
// as recipient in bot methods. It is very useful in cases where
// you have special group IDs, for example in your config, and don't
// want to wrap it into *tele.Chat every time you send messages.
//
// Example:
//
//		group := tele.ChatID(-100756389456)
//		b.Send(group, "Hello!")
//
//		type Config struct {
//			AdminGroup tele.ChatID `json:"admin_group"`
//		}
//		b.Send(conf.AdminGroup, "Hello!")
//
type ChatID int64

// Recipient returns chat ID (see Recipient interface).
func (i ChatID) Recipient() string {
	return strconv.FormatInt(int64(i), 10)
}

// ChatJoinRequest represents a join request sent to a chat.
type ChatJoinRequest struct {
	// Chat to which the request was sent.
	Chat *Chat `json:"chat"`

	// Sender is the user that sent the join request.
	Sender *User `json:"from"`

	// Unixtime, use ChatJoinRequest.Time() to get time.Time.
	Unixtime int64 `json:"date"`

	// Bio of the user, optional.
	Bio string `json:"bio"`

	// InviteLink is the chat invite link that was used by
	//the user to send the join request, optional.
	InviteLink *ChatInviteLink `json:"invite_link"`
}

// Time returns the moment of chat join request sending in local time.
func (r ChatJoinRequest) Time() time.Time {
	return time.Unix(r.Unixtime, 0)
}

// CreateInviteLink creates an additional invite link for a chat.
func (b *Bot) CreateInviteLink(chat Recipient, link *ChatInviteLink) (*ChatInviteLink, error) {
	params := map[string]string{
		"chat_id": chat.Recipient(),
	}
	if link != nil {
		params["name"] = link.Name

		if link.ExpireUnixtime != 0 {
			params["expire_date"] = strconv.FormatInt(link.ExpireUnixtime, 10)
		}
		if link.MemberLimit > 0 {
			params["member_limit"] = strconv.Itoa(link.MemberLimit)
		} else if link.JoinRequest {
			params["creates_join_request"] = "true"
		}
	}

	data, err := b.Raw("createChatInviteLink", params)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result ChatInviteLink `json:"result"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, wrapError(err)
	}

	return &resp.Result, nil
}

// EditInviteLink edits a non-primary invite link created by the bot.
func (b *Bot) EditInviteLink(chat Recipient, link *ChatInviteLink) (*ChatInviteLink, error) {
	params := map[string]string{
		"chat_id": chat.Recipient(),
	}
	if link != nil {
		params["invite_link"] = link.InviteLink
		params["name"] = link.Name

		if link.ExpireUnixtime != 0 {
			params["expire_date"] = strconv.FormatInt(link.ExpireUnixtime, 10)
		}
		if link.MemberLimit > 0 {
			params["member_limit"] = strconv.Itoa(link.MemberLimit)
		} else if link.JoinRequest {
			params["creates_join_request"] = "true"
		}
	}

	data, err := b.Raw("editChatInviteLink", params)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result ChatInviteLink `json:"result"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, wrapError(err)
	}

	return &resp.Result, nil
}

// RevokeInviteLink revokes an invite link created by the bot.
func (b *Bot) RevokeInviteLink(chat Recipient, link string) (*ChatInviteLink, error) {
	params := map[string]string{
		"chat_id":     chat.Recipient(),
		"invite_link": link,
	}

	data, err := b.Raw("revokeChatInviteLink", params)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result ChatInviteLink `json:"result"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, wrapError(err)
	}

	return &resp.Result, nil
}

// ApproveChatJoinRequest approves a chat join request.
func (b *Bot) ApproveChatJoinRequest(chat Recipient, user *User) error {
	params := map[string]string{
		"chat_id": chat.Recipient(),
		"user_id": user.Recipient(),
	}

	data, err := b.Raw("approveChatJoinRequest", params)
	if err != nil {
		return err
	}

	return extractOk(data)
}

// DeclineChatJoinRequest declines a chat join request.
func (b *Bot) DeclineChatJoinRequest(chat Recipient, user *User) error {
	params := map[string]string{
		"chat_id": chat.Recipient(),
		"user_id": user.Recipient(),
	}

	data, err := b.Raw("declineChatJoinRequest", params)
	if err != nil {
		return err
	}

	return extractOk(data)
}
