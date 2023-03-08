package linodego

import (
	"context"
	"encoding/json"
)

type SecurityQuestion struct {
	ID       int    `json:"id"`
	Question string `json:"question"`
	Response string `json:"response"`
}

type SecurityQuestionsListResponse struct {
	SecurityQuestions []SecurityQuestion `json:"security_questions"`
}

type SecurityQuestionsAnswerQuestion struct {
	QuestionID int    `json:"question_id"`
	Response   string `json:"response"`
}

type SecurityQuestionsAnswerOptions struct {
	SecurityQuestions []SecurityQuestionsAnswerQuestion `json:"security_questions"`
}

// SecurityQuestionsList returns a collection of security questions and their responses, if any, for your User Profile.
func (c *Client) SecurityQuestionsList(ctx context.Context) (*SecurityQuestionsListResponse, error) {
	e := "profile/security-questions"
	req := c.R(ctx).SetResult(&SecurityQuestionsListResponse{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*SecurityQuestionsListResponse), nil
}

// SecurityQuestionsAnswer adds security question responses for your User.
func (c *Client) SecurityQuestionsAnswer(ctx context.Context, opts SecurityQuestionsAnswerOptions) error {
	body, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	e := "profile/security-questions"
	req := c.R(ctx).SetBody(string(body))
	_, err = coupleAPIErrors(req.Post(e))
	return err
}
