package telebot

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Option is a shortcut flag type for certain message features
// (so-called options). It means that instead of passing
// fully-fledged SendOptions* to Send(), you can use these
// flags instead.
//
// Supported options are defined as iota-constants.
//
type Option int

const (
	// NoPreview = SendOptions.DisableWebPagePreview
	NoPreview Option = iota

	// Silent = SendOptions.DisableNotification
	Silent

	// AllowWithoutReply = SendOptions.AllowWithoutReply
	AllowWithoutReply

	// Protected = SendOptions.Protected
	Protected

	// ForceReply = ReplyMarkup.ForceReply
	ForceReply

	// OneTimeKeyboard = ReplyMarkup.OneTimeKeyboard
	OneTimeKeyboard

	// RemoveKeyboard = ReplyMarkup.RemoveKeyboard
	RemoveKeyboard
)

// Placeholder is used to set input field placeholder as a send option.
func Placeholder(text string) *SendOptions {
	return &SendOptions{
		ReplyMarkup: &ReplyMarkup{
			ForceReply:  true,
			Placeholder: text,
		},
	}
}

// SendOptions has most complete control over in what way the message
// must be sent, providing an API-complete set of custom properties
// and options.
//
// Despite its power, SendOptions is rather inconvenient to use all
// the way through bot logic, so you might want to consider storing
// and re-using it somewhere or be using Option flags instead.
//
type SendOptions struct {
	// If the message is a reply, original message.
	ReplyTo *Message

	// See ReplyMarkup struct definition.
	ReplyMarkup *ReplyMarkup

	// For text messages, disables previews for links in this message.
	DisableWebPagePreview bool

	// Sends the message silently. iOS users will not receive a notification, Android users will receive a notification with no sound.
	DisableNotification bool

	// ParseMode controls how client apps render your message.
	ParseMode ParseMode

	// Entities is a list of special entities that appear in message text, which can be specified instead of parse_mode.
	Entities Entities

	// AllowWithoutReply allows sending messages not a as reply if the replied-to message has already been deleted.
	AllowWithoutReply bool

	// Protected protects the contents of the sent message from forwarding and saving
	Protected bool
}

func (og *SendOptions) copy() *SendOptions {
	cp := *og
	if cp.ReplyMarkup != nil {
		cp.ReplyMarkup = cp.ReplyMarkup.copy()
	}
	return &cp
}

// ReplyMarkup controls two convenient options for bot-user communications
// such as reply keyboard and inline "keyboard" (a grid of buttons as a part
// of the message).
type ReplyMarkup struct {
	// InlineKeyboard is a grid of InlineButtons displayed in the message.
	//
	// Note: DO NOT confuse with ReplyKeyboard and other keyboard properties!
	InlineKeyboard [][]InlineButton `json:"inline_keyboard,omitempty"`

	// ReplyKeyboard is a grid, consisting of keyboard buttons.
	//
	// Note: you don't need to set HideCustomKeyboard field to show custom keyboard.
	ReplyKeyboard [][]ReplyButton `json:"keyboard,omitempty"`

	// ForceReply forces Telegram clients to display
	// a reply interface to the user (act as if the user
	// has selected the bot‘s message and tapped "Reply").
	ForceReply bool `json:"force_reply,omitempty"`

	// Requests clients to resize the keyboard vertically for optimal fit
	// (e.g. make the keyboard smaller if there are just two rows of buttons).
	//
	// Defaults to false, in which case the custom keyboard is always of the
	// same height as the app's standard keyboard.
	ResizeKeyboard bool `json:"resize_keyboard,omitempty"`

	// Requests clients to hide the reply keyboard as soon as it's been used.
	//
	// Defaults to false.
	OneTimeKeyboard bool `json:"one_time_keyboard,omitempty"`

	// Requests clients to remove the reply keyboard.
	//
	// Defaults to false.
	RemoveKeyboard bool `json:"remove_keyboard,omitempty"`

	// Use this param if you want to force reply from
	// specific users only.
	//
	// Targets:
	// 1) Users that are @mentioned in the text of the Message object;
	// 2) If the bot's message is a reply (has SendOptions.ReplyTo),
	//       sender of the original message.
	Selective bool `json:"selective,omitempty"`

	// Placeholder will be shown in the input field when the reply is active.
	Placeholder string `json:"input_field_placeholder,omitempty"`
}

func (r *ReplyMarkup) copy() *ReplyMarkup {
	cp := *r

	if len(r.ReplyKeyboard) > 0 {
		cp.ReplyKeyboard = make([][]ReplyButton, len(r.ReplyKeyboard))
		for i, row := range r.ReplyKeyboard {
			cp.ReplyKeyboard[i] = make([]ReplyButton, len(row))
			copy(cp.ReplyKeyboard[i], row)
		}
	}

	if len(r.InlineKeyboard) > 0 {
		cp.InlineKeyboard = make([][]InlineButton, len(r.InlineKeyboard))
		for i, row := range r.InlineKeyboard {
			cp.InlineKeyboard[i] = make([]InlineButton, len(row))
			copy(cp.InlineKeyboard[i], row)
		}
	}

	return &cp
}

// ReplyButton represents a button displayed in reply-keyboard.
//
// Set either Contact or Location to true in order to request
// sensitive info, such as user's phone number or current location.
//
type ReplyButton struct {
	Text string `json:"text"`

	Contact  bool     `json:"request_contact,omitempty"`
	Location bool     `json:"request_location,omitempty"`
	Poll     PollType `json:"request_poll,omitempty"`
}

// MarshalJSON implements json.Marshaler. It allows to pass
// PollType as keyboard's poll type instead of KeyboardButtonPollType object.
func (pt PollType) MarshalJSON() ([]byte, error) {
	var aux = struct {
		Type string `json:"type"`
	}{
		Type: string(pt),
	}
	return json.Marshal(&aux)
}

// Row represents an array of buttons, a row.
type Row []Btn

// Row creates a row of buttons.
func (r *ReplyMarkup) Row(many ...Btn) Row {
	return many
}

// Split splits the keyboard into the rows with N maximum number of buttons.
// For example, if you pass six buttons and 3 as the max, you get two rows with
// three buttons in each.
//
// `Split(3, []Btn{six buttons...}) -> [[1, 2, 3], [4, 5, 6]]`
// `Split(2, []Btn{six buttons...}) -> [[1, 2],[3, 4],[5, 6]]`
//
func (r *ReplyMarkup) Split(max int, btns []Btn) []Row {
	rows := make([]Row, (max-1+len(btns))/max)
	for i, b := range btns {
		i /= max
		rows[i] = append(rows[i], b)
	}
	return rows
}

func (r *ReplyMarkup) Inline(rows ...Row) {
	inlineKeys := make([][]InlineButton, 0, len(rows))
	for i, row := range rows {
		keys := make([]InlineButton, 0, len(row))
		for j, btn := range row {
			btn := btn.Inline()
			if btn == nil {
				panic(fmt.Sprintf(
					"telebot: button row %d column %d is not an inline button",
					i, j))
			}
			keys = append(keys, *btn)
		}
		inlineKeys = append(inlineKeys, keys)
	}

	r.InlineKeyboard = inlineKeys
}

func (r *ReplyMarkup) Reply(rows ...Row) {
	replyKeys := make([][]ReplyButton, 0, len(rows))
	for i, row := range rows {
		keys := make([]ReplyButton, 0, len(row))
		for j, btn := range row {
			btn := btn.Reply()
			if btn == nil {
				panic(fmt.Sprintf(
					"telebot: button row %d column %d is not a reply button",
					i, j))
			}
			keys = append(keys, *btn)
		}
		replyKeys = append(replyKeys, keys)
	}

	r.ReplyKeyboard = replyKeys
}

func (r *ReplyMarkup) Text(text string) Btn {
	return Btn{Text: text}
}

func (r *ReplyMarkup) Contact(text string) Btn {
	return Btn{Contact: true, Text: text}
}

func (r *ReplyMarkup) Location(text string) Btn {
	return Btn{Location: true, Text: text}
}

func (r *ReplyMarkup) Poll(text string, poll PollType) Btn {
	return Btn{Poll: poll, Text: text}
}

func (r *ReplyMarkup) Data(text, unique string, data ...string) Btn {
	return Btn{
		Unique: unique,
		Text:   text,
		Data:   strings.Join(data, "|"),
	}
}

func (r *ReplyMarkup) URL(text, url string) Btn {
	return Btn{Text: text, URL: url}
}

func (r *ReplyMarkup) Query(text, query string) Btn {
	return Btn{Text: text, InlineQuery: query}
}

func (r *ReplyMarkup) QueryChat(text, query string) Btn {
	return Btn{Text: text, InlineQueryChat: query}
}

func (r *ReplyMarkup) Login(text string, login *Login) Btn {
	return Btn{Login: login, Text: text}
}

// Btn is a constructor button, which will later become either a reply, or an inline button.
type Btn struct {
	Unique          string   `json:"unique,omitempty"`
	Text            string   `json:"text,omitempty"`
	URL             string   `json:"url,omitempty"`
	Data            string   `json:"callback_data,omitempty"`
	InlineQuery     string   `json:"switch_inline_query,omitempty"`
	InlineQueryChat string   `json:"switch_inline_query_current_chat,omitempty"`
	Contact         bool     `json:"request_contact,omitempty"`
	Location        bool     `json:"request_location,omitempty"`
	Poll            PollType `json:"request_poll,omitempty"`
	Login           *Login   `json:"login_url,omitempty"`
}

func (b Btn) Inline() *InlineButton {
	return &InlineButton{
		Unique:          b.Unique,
		Text:            b.Text,
		URL:             b.URL,
		Data:            b.Data,
		InlineQuery:     b.InlineQuery,
		InlineQueryChat: b.InlineQueryChat,
		Login:           b.Login,
	}
}

func (b Btn) Reply() *ReplyButton {
	if b.Unique != "" {
		return nil
	}

	return &ReplyButton{
		Text:     b.Text,
		Contact:  b.Contact,
		Location: b.Location,
		Poll:     b.Poll,
	}
}

// CommandParams controls parameters for commands-related methods (setMyCommands, deleteMyCommands and getMyCommands).
type CommandParams struct {
	Commands     []Command     `json:"commands,omitempty"`
	Scope        *CommandScope `json:"scope,omitempty"`
	LanguageCode string        `json:"language_code,omitempty"`
}

// CommandScope object represents a scope to which bot commands are applied.
type CommandScope struct {
	Type   string `json:"type"`
	ChatID int64  `json:"chat_id,omitempty"`
	UserID int64  `json:"user_id,omitempty"`
}

// CommandScope types
const (
	CommandScopeDefault         = "default"
	CommandScopeAllPrivateChats = "all_private_chats"
	CommandScopeAllGroupChats   = "all_group_chats"
	CommandScopeAllChatAdmin    = "all_chat_administrators"
	CommandScopeChat            = "chat"
	CommandScopeChatAdmin       = "chat_administrators"
	CommandScopeChatMember      = "chat_member"
)
