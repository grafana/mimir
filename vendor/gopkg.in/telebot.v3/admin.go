package telebot

import (
	"encoding/json"
	"strconv"
	"time"
)

// ChatInviteLink object represents an invite for a chat.
type ChatInviteLink struct {
	// The invite link.
	InviteLink string `json:"invite_link"`

	// Invite link name.
	Name string `json:"name"`

	// The creator of the link.
	Creator *User `json:"creator"`

	// If the link is primary.
	IsPrimary bool `json:"is_primary"`

	// If the link is revoked.
	IsRevoked bool `json:"is_revoked"`

	// (Optional) Point in time when the link will expire,
	// use ExpireDate() to get time.Time.
	ExpireUnixtime int64 `json:"expire_date,omitempty"`

	// (Optional) Maximum number of users that can be members of
	// the chat simultaneously.
	MemberLimit int `json:"member_limit,omitempty"`

	// (Optional) True, if users joining the chat via the link need to
	// be approved by chat administrators. If True, member_limit can't be specified.
	JoinRequest bool `json:"creates_join_request"`

	// (Optional) Number of pending join requests created using this link.
	PendingCount int `json:"pending_join_request_count"`
}

// ExpireDate returns the moment of the link expiration in local time.
func (c *ChatInviteLink) ExpireDate() time.Time {
	return time.Unix(c.ExpireUnixtime, 0)
}

// ChatMemberUpdate object represents changes in the status of a chat member.
type ChatMemberUpdate struct {
	// Chat where the user belongs to.
	Chat *Chat `json:"chat"`

	// Sender which user the action was triggered.
	Sender *User `json:"from"`

	// Unixtime, use Date() to get time.Time.
	Unixtime int64 `json:"date"`

	// Previous information about the chat member.
	OldChatMember *ChatMember `json:"old_chat_member"`

	// New information about the chat member.
	NewChatMember *ChatMember `json:"new_chat_member"`

	// (Optional) InviteLink which was used by the user to
	// join the chat; for joining by invite link events only.
	InviteLink *ChatInviteLink `json:"invite_link"`
}

// Time returns the moment of the change in local time.
func (c *ChatMemberUpdate) Time() time.Time {
	return time.Unix(c.Unixtime, 0)
}

// Rights is a list of privileges available to chat members.
type Rights struct {
	CanBeEdited         bool `json:"can_be_edited"`
	CanChangeInfo       bool `json:"can_change_info"`
	CanPostMessages     bool `json:"can_post_messages"`
	CanEditMessages     bool `json:"can_edit_messages"`
	CanDeleteMessages   bool `json:"can_delete_messages"`
	CanInviteUsers      bool `json:"can_invite_users"`
	CanRestrictMembers  bool `json:"can_restrict_members"`
	CanPinMessages      bool `json:"can_pin_messages"`
	CanPromoteMembers   bool `json:"can_promote_members"`
	CanSendMessages     bool `json:"can_send_messages"`
	CanSendMedia        bool `json:"can_send_media_messages"`
	CanSendPolls        bool `json:"can_send_polls"`
	CanSendOther        bool `json:"can_send_other_messages"`
	CanAddPreviews      bool `json:"can_add_web_page_previews"`
	CanManageVoiceChats bool `json:"can_manage_voice_chats"`
	CanManageChat       bool `json:"can_manage_chat"`
}

// NoRights is the default Rights{}.
func NoRights() Rights { return Rights{} }

// NoRestrictions should be used when un-restricting or
// un-promoting user.
//
//		member.Rights = tele.NoRestrictions()
//		b.Restrict(chat, member)
//
func NoRestrictions() Rights {
	return Rights{
		CanBeEdited:         true,
		CanChangeInfo:       false,
		CanPostMessages:     false,
		CanEditMessages:     false,
		CanDeleteMessages:   false,
		CanInviteUsers:      false,
		CanRestrictMembers:  false,
		CanPinMessages:      false,
		CanPromoteMembers:   false,
		CanSendMessages:     true,
		CanSendMedia:        true,
		CanSendPolls:        true,
		CanSendOther:        true,
		CanAddPreviews:      true,
		CanManageVoiceChats: false,
		CanManageChat:       false,
	}
}

// AdminRights could be used to promote user to admin.
func AdminRights() Rights {
	return Rights{
		CanBeEdited:         true,
		CanChangeInfo:       true,
		CanPostMessages:     true,
		CanEditMessages:     true,
		CanDeleteMessages:   true,
		CanInviteUsers:      true,
		CanRestrictMembers:  true,
		CanPinMessages:      true,
		CanPromoteMembers:   true,
		CanSendMessages:     true,
		CanSendMedia:        true,
		CanSendPolls:        true,
		CanSendOther:        true,
		CanAddPreviews:      true,
		CanManageVoiceChats: true,
		CanManageChat:       true,
	}
}

// Forever is a ExpireUnixtime of "forever" banning.
func Forever() int64 {
	return time.Now().Add(367 * 24 * time.Hour).Unix()
}

// Ban will ban user from chat until `member.RestrictedUntil`.
func (b *Bot) Ban(chat *Chat, member *ChatMember, revokeMessages ...bool) error {
	params := map[string]string{
		"chat_id":    chat.Recipient(),
		"user_id":    member.User.Recipient(),
		"until_date": strconv.FormatInt(member.RestrictedUntil, 10),
	}
	if len(revokeMessages) > 0 {
		params["revoke_messages"] = strconv.FormatBool(revokeMessages[0])
	}

	_, err := b.Raw("kickChatMember", params)
	return err
}

// Unban will unban user from chat, who would have thought eh?
// forBanned does nothing if the user is not banned.
func (b *Bot) Unban(chat *Chat, user *User, forBanned ...bool) error {
	params := map[string]string{
		"chat_id": chat.Recipient(),
		"user_id": user.Recipient(),
	}

	if len(forBanned) > 0 {
		params["only_if_banned"] = strconv.FormatBool(forBanned[0])
	}

	_, err := b.Raw("unbanChatMember", params)
	return err
}

// Restrict lets you restrict a subset of member's rights until
// member.RestrictedUntil, such as:
//
//     * can send messages
//     * can send media
//     * can send other
//     * can add web page previews
//
func (b *Bot) Restrict(chat *Chat, member *ChatMember) error {
	prv, until := member.Rights, member.RestrictedUntil

	params := map[string]interface{}{
		"chat_id":    chat.Recipient(),
		"user_id":    member.User.Recipient(),
		"until_date": strconv.FormatInt(until, 10),
	}
	embedRights(params, prv)

	_, err := b.Raw("restrictChatMember", params)
	return err
}

// Promote lets you update member's admin rights, such as:
//
//     * can change info
//     * can post messages
//     * can edit messages
//     * can delete messages
//     * can invite users
//     * can restrict members
//     * can pin messages
//     * can promote members
//
func (b *Bot) Promote(chat *Chat, member *ChatMember) error {
	prv := member.Rights

	params := map[string]interface{}{
		"chat_id":      chat.Recipient(),
		"user_id":      member.User.Recipient(),
		"is_anonymous": member.Anonymous,
	}
	embedRights(params, prv)

	_, err := b.Raw("promoteChatMember", params)
	return err
}

// AdminsOf returns a member list of chat admins.
//
// On success, returns an Array of ChatMember objects that
// contains information about all chat administrators except other bots.
//
// If the chat is a group or a supergroup and
// no administrators were appointed, only the creator will be returned.
//
func (b *Bot) AdminsOf(chat *Chat) ([]ChatMember, error) {
	params := map[string]string{
		"chat_id": chat.Recipient(),
	}

	data, err := b.Raw("getChatAdministrators", params)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result []ChatMember
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, wrapError(err)
	}
	return resp.Result, nil
}

// Len returns the number of members in a chat.
func (b *Bot) Len(chat *Chat) (int, error) {
	params := map[string]string{
		"chat_id": chat.Recipient(),
	}

	data, err := b.Raw("getChatMembersCount", params)
	if err != nil {
		return 0, err
	}

	var resp struct {
		Result int
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return 0, wrapError(err)
	}
	return resp.Result, nil
}

// SetAdminTitle sets a custom title for an administrator.
// A title should be 0-16 characters length, emoji are not allowed.
func (b *Bot) SetAdminTitle(chat *Chat, user *User, title string) error {
	params := map[string]string{
		"chat_id":      chat.Recipient(),
		"user_id":      user.Recipient(),
		"custom_title": title,
	}

	_, err := b.Raw("setChatAdministratorCustomTitle", params)
	return err
}

// BanSenderChat will use this method to ban a channel chat in a supergroup or a channel.
// Until the chat is unbanned, the owner of the banned chat won't be able
// to send messages on behalf of any of their channels.
func (b *Bot) BanSenderChat(chat *Chat, sender Recipient) error {
	params := map[string]string{
		"chat_id":        chat.Recipient(),
		"sender_chat_id": sender.Recipient(),
	}

	_, err := b.Raw("banChatSenderChat", params)
	return err
}

// UnbanSenderChat will use this method to unban a previously banned channel chat in a supergroup or channel.
// The bot must be an administrator for this to work and must have the appropriate administrator rights.
func (b *Bot) UnbanSenderChat(chat *Chat, sender Recipient) error {
	params := map[string]string{
		"chat_id":        chat.Recipient(),
		"sender_chat_id": sender.Recipient(),
	}

	_, err := b.Raw("unbanChatSenderChat", params)
	return err
}
