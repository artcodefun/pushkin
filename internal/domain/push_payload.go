package domain

import (
	"fmt"
	"net/url"
	"strings"
)

// PushPayload is the mobile-push notification payload supported in v1.
// Provider-specific size limits are checked by the application layer.
type PushPayload struct {
	title    string
	body     string
	imageURL string
	data     map[string]string
}

func NewPushPayload(title, body, imageURL string, data map[string]string) (PushPayload, error) {
	dataCopy := make(map[string]string, len(data))
	for key, value := range data {
		dataCopy[key] = value
	}

	payload := PushPayload{
		title:    title,
		body:     body,
		imageURL: imageURL,
		data:     dataCopy,
	}
	if err := payload.validate(); err != nil {
		return PushPayload{}, err
	}

	return payload, nil
}

func (p PushPayload) Title() string    { return p.title }
func (p PushPayload) Body() string     { return p.body }
func (p PushPayload) ImageURL() string { return p.imageURL }

func (p PushPayload) Data() map[string]string {
	result := make(map[string]string, len(p.data))
	for key, value := range p.data {
		result[key] = value
	}

	return result
}

func (p PushPayload) validate() error {
	if strings.TrimSpace(p.title) == "" && strings.TrimSpace(p.body) == "" && len(p.data) == 0 {
		return fmt.Errorf("%w: payload must contain notification text or data", ErrInvalidArgument)
	}

	if p.imageURL != "" {
		parsed, err := url.Parse(p.imageURL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("%w: image must be an absolute HTTP(S) URL", ErrInvalidArgument)
		}
	}

	for key := range p.data {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%w: payload data key must not be empty", ErrInvalidArgument)
		}
	}

	return nil
}
