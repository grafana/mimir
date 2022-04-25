// Package telebot is a framework for Telegram bots.
//
// Example:
//
//		package main
//
//		import (
//			"time"
//			tele "gopkg.in/tucnak/telebot.v3"
//		)
//
//		func main() {
//			b, err := tele.NewBot(tele.Settings{
//				Token:  "...",
//				Poller: &tele.LongPoller{Timeout: 10 * time.Second},
//			})
//			if err != nil {
//				return
//			}
//
//			b.Handle(tele.OnText, func(c tele.Context) error {
//				return c.Send("Hello world!")
//			})
//
//			b.Start()
//		}
//
package telebot

import "errors"

var (
	ErrBadRecipient    = errors.New("telebot: recipient is nil")
	ErrUnsupportedWhat = errors.New("telebot: unsupported what argument")
	ErrCouldNotUpdate  = errors.New("telebot: could not fetch new updates")
	ErrTrueResult      = errors.New("telebot: result is True")
	ErrBadContext      = errors.New("telebot: context does not contain message")
)

const DefaultApiURL = "https://api.telegram.org"

// These are one of the possible events Handle() can deal with.
//
// For convenience, all Telebot-provided endpoints start with
// an "alert" character \a.
//
const (
	// Basic message handlers.
	OnText       = "\atext"
	OnEdited     = "\aedited"
	OnPhoto      = "\aphoto"
	OnAudio      = "\aaudio"
	OnAnimation  = "\aanimation"
	OnDocument   = "\adocument"
	OnSticker    = "\asticker"
	OnVideo      = "\avideo"
	OnVoice      = "\avoice"
	OnVideoNote  = "\avideo_note"
	OnContact    = "\acontact"
	OnLocation   = "\alocation"
	OnVenue      = "\avenue"
	OnDice       = "\adice"
	OnInvoice    = "\ainvoice"
	OnPayment    = "\apayment"
	OnGame       = "\agame"
	OnPoll       = "\apoll"
	OnPollAnswer = "\apoll_answer"
	OnPinned     = "\apinned"

	// Will fire on channel posts.
	OnChannelPost       = "\achannel_post"
	OnEditedChannelPost = "\aedited_channel_post"

	// Will fire when bot is added to a group.
	OnAddedToGroup = "\aadded_to_group"

	// Service events:
	OnUserJoined        = "\auser_joined"
	OnUserLeft          = "\auser_left"
	OnNewGroupTitle     = "\anew_chat_title"
	OnNewGroupPhoto     = "\anew_chat_photo"
	OnGroupPhotoDeleted = "\achat_photo_deleted"
	OnGroupCreated      = "\agroup_created"
	OnSuperGroupCreated = "\asupergroup_created"
	OnChannelCreated    = "\achannel_created"

	// Migration happens when group switches to
	// a supergroup. You might want to update
	// your internal references to this chat
	// upon switching as its ID will change.
	OnMigration = "\amigration"

	// Will fire on any unhandled media.
	OnMedia = "\amedia"

	// Will fire on callback requests.
	OnCallback = "\acallback"

	// Will fire on incoming inline queries.
	OnQuery = "\aquery"

	// Will fire on chosen inline results.
	OnInlineResult = "\ainline_result"

	// Will fire on a shipping query.
	OnShipping = "\ashipping_query"

	// Will fire on pre checkout query.
	OnCheckout = "\apre_checkout_query"

	// Will fire on bot's chat member changes.
	OnMyChatMember = "\amy_chat_member"

	// Will fire on chat member's changes.
	OnChatMember = "\achat_member"

	// Will fire on chat join request.
	OnChatJoinRequest = "\achat_join_request"

	// Will fire on the start of a voice chat.
	OnVoiceChatStarted = "\avoice_chat_started"

	// Will fire on the end of a voice chat.
	OnVoiceChatEnded = "\avoice_chat_ended"

	// Will fire on invited participants to the voice chat.
	OnVoiceChatParticipants = "\avoice_chat_participants_invited"

	// Will fire on scheduling a voice chat.
	OnVoiceChatScheduled = "\avoice_chat_scheduled"

	// Will fire on a proximity alert.
	OnProximityAlert = "\aproximity_alert_triggered"

	// Will fire on auto delete timer set.
	OnAutoDeleteTimer = "\amessage_auto_delete_timer_changed"
)

// ChatAction is a client-side status indicating bot activity.
type ChatAction string

const (
	Typing            ChatAction = "typing"
	UploadingPhoto    ChatAction = "upload_photo"
	UploadingVideo    ChatAction = "upload_video"
	UploadingAudio    ChatAction = "upload_audio"
	UploadingDocument ChatAction = "upload_document"
	UploadingVNote    ChatAction = "upload_video_note"
	RecordingVideo    ChatAction = "record_video"
	RecordingAudio    ChatAction = "record_audio"
	RecordingVNote    ChatAction = "record_video_note"
	FindingLocation   ChatAction = "find_location"
	ChoosingSticker   ChatAction = "choose_sticker"
)

// ParseMode determines the way client applications treat the text of the message
type ParseMode = string

const (
	ModeDefault    ParseMode = ""
	ModeMarkdown   ParseMode = "Markdown"
	ModeMarkdownV2 ParseMode = "MarkdownV2"
	ModeHTML       ParseMode = "HTML"
)

// EntityType is a MessageEntity type.
type EntityType string

const (
	EntityMention       EntityType = "mention"
	EntityTMention      EntityType = "text_mention"
	EntityHashtag       EntityType = "hashtag"
	EntityCashtag       EntityType = "cashtag"
	EntityCommand       EntityType = "bot_command"
	EntityURL           EntityType = "url"
	EntityEmail         EntityType = "email"
	EntityPhone         EntityType = "phone_number"
	EntityBold          EntityType = "bold"
	EntityItalic        EntityType = "italic"
	EntityUnderline     EntityType = "underline"
	EntityStrikethrough EntityType = "strikethrough"
	EntityCode          EntityType = "code"
	EntityCodeBlock     EntityType = "pre"
	EntityTextLink      EntityType = "text_link"
	EntitySpoiler       EntityType = "spoiler"
)

// ChatType represents one of the possible chat types.
type ChatType string

const (
	ChatPrivate        ChatType = "private"
	ChatGroup          ChatType = "group"
	ChatSuperGroup     ChatType = "supergroup"
	ChatChannel        ChatType = "channel"
	ChatChannelPrivate ChatType = "privatechannel"
)

// MemberStatus is one's chat status.
type MemberStatus string

const (
	Creator       MemberStatus = "creator"
	Administrator MemberStatus = "administrator"
	Member        MemberStatus = "member"
	Restricted    MemberStatus = "restricted"
	Left          MemberStatus = "left"
	Kicked        MemberStatus = "kicked"
)

// MaskFeature defines sticker mask position.
type MaskFeature string

const (
	FeatureForehead MaskFeature = "forehead"
	FeatureEyes     MaskFeature = "eyes"
	FeatureMouth    MaskFeature = "mouth"
	FeatureChin     MaskFeature = "chin"
)

// PollType defines poll types.
type PollType string

const (
	// Despite "any" type isn't described in documentation,
	// it needed for proper KeyboardButtonPollType marshaling.
	PollAny PollType = "any"

	PollQuiz    PollType = "quiz"
	PollRegular PollType = "regular"
)

type DiceType string

var (
	Cube = &Dice{Type: "🎲"}
	Dart = &Dice{Type: "🎯"}
	Ball = &Dice{Type: "🏀"}
	Goal = &Dice{Type: "⚽"}
	Slot = &Dice{Type: "🎰"}
	Bowl = &Dice{Type: "🎳"}
)
