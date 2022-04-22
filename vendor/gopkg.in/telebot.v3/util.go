package telebot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

var defaultOnError = func(err error, c Context) {
	log.Println(c.Update().ID, err)
}

func (b *Bot) debug(err error) {
	log.Println(err)
}

func (b *Bot) deferDebug() {
	if r := recover(); r != nil {
		if err, ok := r.(error); ok {
			b.debug(err)
		} else if str, ok := r.(string); ok {
			b.debug(fmt.Errorf("%s", str))
		}
	}
}

func (b *Bot) runHandler(h HandlerFunc, c Context) {
	f := func() {
		defer b.deferDebug()
		if err := h(c); err != nil {
			b.OnError(err, c)
		}
	}
	if b.synchronous {
		f()
	} else {
		go f()
	}
}

func applyMiddleware(h HandlerFunc, middleware ...MiddlewareFunc) HandlerFunc {
	for i := len(middleware) - 1; i >= 0; i-- {
		h = middleware[i](h)
	}
	return h
}

// wrapError returns new wrapped telebot-related error.
func wrapError(err error) error {
	return fmt.Errorf("telebot: %w", err)
}

// extractOk checks given result for error. If result is ok returns nil.
// In other cases it extracts API error. If error is not presented
// in errors.go, it will be prefixed with `unknown` keyword.
func extractOk(data []byte) error {
	var e struct {
		Ok          bool                   `json:"ok"`
		Code        int                    `json:"error_code"`
		Description string                 `json:"description"`
		Parameters  map[string]interface{} `json:"parameters"`
	}
	if json.NewDecoder(bytes.NewReader(data)).Decode(&e) != nil {
		return nil // FIXME
	}
	if e.Ok {
		return nil
	}

	err := Err(e.Description)
	switch err {
	case nil:
	case ErrGroupMigrated:
		migratedTo, ok := e.Parameters["migrate_to_chat_id"]
		if !ok {
			return NewError(e.Code, e.Description)
		}

		return GroupError{
			err:        err.(*Error),
			MigratedTo: int64(migratedTo.(float64)),
		}
	default:
		return err
	}

	switch e.Code {
	case http.StatusTooManyRequests:
		retryAfter, ok := e.Parameters["retry_after"]
		if !ok {
			return NewError(e.Code, e.Description)
		}

		err = FloodError{
			err:        NewError(e.Code, e.Description),
			RetryAfter: int(retryAfter.(float64)),
		}
	default:
		err = fmt.Errorf("telegram: %s (%d)", e.Description, e.Code)
	}

	return err
}

// extractMessage extracts common Message result from given data.
// Should be called after extractOk or b.Raw() to handle possible errors.
func extractMessage(data []byte) (*Message, error) {
	var resp struct {
		Result *Message
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		var resp struct {
			Result bool
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			return nil, wrapError(err)
		}
		if resp.Result {
			return nil, ErrTrueResult
		}
		return nil, wrapError(err)
	}
	return resp.Result, nil
}

func extractOptions(how []interface{}) *SendOptions {
	opts := &SendOptions{}

	for _, prop := range how {
		switch opt := prop.(type) {
		case *SendOptions:
			opts = opt.copy()
		case *ReplyMarkup:
			if opt != nil {
				opts.ReplyMarkup = opt.copy()
			}
		case Option:
			switch opt {
			case NoPreview:
				opts.DisableWebPagePreview = true
			case Silent:
				opts.DisableNotification = true
			case AllowWithoutReply:
				opts.AllowWithoutReply = true
			case ForceReply:
				if opts.ReplyMarkup == nil {
					opts.ReplyMarkup = &ReplyMarkup{}
				}
				opts.ReplyMarkup.ForceReply = true
			case OneTimeKeyboard:
				if opts.ReplyMarkup == nil {
					opts.ReplyMarkup = &ReplyMarkup{}
				}
				opts.ReplyMarkup.OneTimeKeyboard = true
			case RemoveKeyboard:
				if opts.ReplyMarkup == nil {
					opts.ReplyMarkup = &ReplyMarkup{}
				}
				opts.ReplyMarkup.RemoveKeyboard = true
			case Protected:
				opts.Protected = true
			default:
				panic("telebot: unsupported flag-option")
			}
		case ParseMode:
			opts.ParseMode = opt
		case Entities:
			opts.Entities = opt
		default:
			panic("telebot: unsupported send-option")
		}
	}

	return opts
}

func (b *Bot) embedSendOptions(params map[string]string, opt *SendOptions) {
	if b.parseMode != ModeDefault {
		params["parse_mode"] = b.parseMode
	}

	if opt == nil {
		return
	}

	if opt.ReplyTo != nil && opt.ReplyTo.ID != 0 {
		params["reply_to_message_id"] = strconv.Itoa(opt.ReplyTo.ID)
	}

	if opt.DisableWebPagePreview {
		params["disable_web_page_preview"] = "true"
	}

	if opt.DisableNotification {
		params["disable_notification"] = "true"
	}

	if opt.ParseMode != ModeDefault {
		params["parse_mode"] = opt.ParseMode
	}

	if len(opt.Entities) > 0 {
		delete(params, "parse_mode")
		entities, _ := json.Marshal(opt.Entities)

		if params["caption"] != "" {
			params["caption_entities"] = string(entities)
		} else {
			params["entities"] = string(entities)
		}
	}

	if opt.AllowWithoutReply {
		params["allow_sending_without_reply"] = "true"
	}

	if opt.ReplyMarkup != nil {
		processButtons(opt.ReplyMarkup.InlineKeyboard)
		replyMarkup, _ := json.Marshal(opt.ReplyMarkup)
		params["reply_markup"] = string(replyMarkup)
	}

	if opt.Protected {
		params["protect_content"] = "true"
	}
}

func processButtons(keys [][]InlineButton) {
	if keys == nil || len(keys) < 1 || len(keys[0]) < 1 {
		return
	}

	for i := range keys {
		for j := range keys[i] {
			key := &keys[i][j]
			if key.Unique != "" {
				// Format: "\f<callback_name>|<data>"
				data := key.Data
				if data == "" {
					key.Data = "\f" + key.Unique
				} else {
					key.Data = "\f" + key.Unique + "|" + data
				}
			}
		}
	}
}

func embedRights(p map[string]interface{}, rights Rights) {
	data, _ := json.Marshal(rights)
	_ = json.Unmarshal(data, &p)
}

func thumbnailToFilemap(thumb *Photo) map[string]File {
	if thumb != nil {
		return map[string]File{"thumb": thumb.File}
	}
	return nil
}

func isUserInList(user *User, list []User) bool {
	for _, user2 := range list {
		if user.ID == user2.ID {
			return true
		}
	}
	return false
}

func intsToStrs(ns []int) (s []string) {
	for _, n := range ns {
		s = append(s, strconv.Itoa(n))
	}
	return
}

// extractCommandsParams extracts parameters for commands-related methods from the given options.
func extractCommandsParams(opts ...interface{}) (params CommandParams) {
	for _, opt := range opts {
		switch value := opt.(type) {
		case []Command:
			params.Commands = value
		case string:
			params.LanguageCode = value
		case CommandScope:
			params.Scope = &value
		}
	}
	return
}
