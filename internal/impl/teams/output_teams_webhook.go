package teams

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	goteamsnotify "github.com/atc0005/go-teams-notify/v2"
	"github.com/atc0005/go-teams-notify/v2/messagecard"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fieldWebhookURL        = "webhook_url"
	fieldTitle             = "title"
	fieldThemeColor        = "theme_color"
	fieldTimeout           = "timeout"
	fieldTLS               = "tls"
	fieldSkipURLValidation = "skip_url_validation"
)

func teamsWebhookOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services", "Social").
		Summary("Sends messages to a Microsoft Teams channel via an incoming webhook.").
		Description(`
Sends messages to a Microsoft Teams channel using the [go-teams-notify](https://github.com/atc0005/go-teams-notify) library.

If the message payload is a valid JSON object matching the [MessageCard](https://learn.microsoft.com/en-us/outlook/actionable-messages/message-card-reference) schema it is sent directly. Otherwise a simple MessageCard is constructed with the raw message content as the card text.

The `+"`title`"+` and `+"`theme_color`"+` fields, when set, always override the values in the card (even when the message is already a valid MessageCard).`).
		Fields(
			service.NewStringField(fieldWebhookURL).
				Description("The Microsoft Teams incoming webhook URL.").
				Secret().
				Example("https://outlook.office.com/webhook/XXXXXXXX/IncomingWebhook/YYYYYYYY/ZZZZZZZZ"),
			service.NewInterpolatedStringField(fieldTitle).
				Description("An optional title for the Teams message card. When set, overrides any title already present in the message.").
				Optional(),
			service.NewStringField(fieldThemeColor).
				Description("An optional hex color code for the left-hand border of the card (e.g. `FF0000` for red). When set, overrides any theme color already present in the message.").
				Default("").
				Advanced(),
			service.NewDurationField(fieldTimeout).
				Description("The maximum time to wait before abandoning a request.").
				Advanced().
				Default("5s"),
			service.NewTLSToggledField(fieldTLS),
			service.NewBoolField(fieldSkipURLValidation).
				Description("When true, disables Microsoft Teams webhook URL validation. Useful for testing with local mock servers.").
				Default(false).
				Advanced(),
		)
}

func init() {
	err := service.RegisterOutput(
		"teams_webhook", teamsWebhookOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			w, err := newTeamsWebhookWriter(conf, mgr)
			return w, 1, err
		},
	)
	if err != nil {
		panic(err)
	}
}

type teamsWebhookWriter struct {
	log *service.Logger

	webhookURL        string
	title             *service.InterpolatedString
	themeColor        string
	httpClient        *http.Client
	skipURLValidation bool

	mu     sync.RWMutex
	client *goteamsnotify.TeamsClient
}

func newTeamsWebhookWriter(conf *service.ParsedConfig, mgr *service.Resources) (*teamsWebhookWriter, error) {
	w := &teamsWebhookWriter{
		log: mgr.Logger(),
	}

	var err error

	if w.webhookURL, err = conf.FieldString(fieldWebhookURL); err != nil {
		return nil, err
	}

	if conf.Contains(fieldTitle) {
		if w.title, err = conf.FieldInterpolatedString(fieldTitle); err != nil {
			return nil, err
		}
	}

	if w.themeColor, err = conf.FieldString(fieldThemeColor); err != nil {
		return nil, err
	}

	timeout, err := conf.FieldDuration(fieldTimeout)
	if err != nil {
		return nil, err
	}

	w.httpClient = &http.Client{Timeout: timeout}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled(fieldTLS)
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		w.httpClient.Transport = &http.Transport{TLSClientConfig: tlsConf}
	}

	if w.skipURLValidation, err = conf.FieldBool(fieldSkipURLValidation); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *teamsWebhookWriter) Connect(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.client != nil {
		return nil
	}

	c := goteamsnotify.NewTeamsClient()
	c.SetHTTPClient(w.httpClient)
	if w.skipURLValidation {
		c.SkipWebhookURLValidationOnSend(true)
	}
	w.client = c

	w.log.Debugf("Connected Teams webhook output to %s", w.webhookURL)
	return nil
}

func (w *teamsWebhookWriter) Write(ctx context.Context, msg *service.Message) error {
	w.mu.RLock()
	client := w.client
	w.mu.RUnlock()

	if client == nil {
		return service.ErrNotConnected
	}

	rawContent, err := msg.AsBytes()
	if err != nil {
		return err
	}

	card, err := w.buildCard(msg, rawContent)
	if err != nil {
		return err
	}

	if sendErr := client.SendWithContext(ctx, w.webhookURL, card); sendErr != nil {
		w.log.Errorf("Failed to send Teams message: %v", sendErr)
		return sendErr
	}

	w.log.Debugf("Message sent to Teams webhook")
	return nil
}

// buildCard constructs a MessageCard from the message payload.
// If the raw content is valid MessageCard JSON it is used as-is; otherwise a
// simple card is created with the payload as plain text.
// Title and ThemeColor config fields always win when set.
func (w *teamsWebhookWriter) buildCard(msg *service.Message, rawContent []byte) (*messagecard.MessageCard, error) {
	card := messagecard.NewMessageCard()

	// Try to parse the payload as an existing MessageCard.
	var parsed messagecard.MessageCard
	if err := json.Unmarshal(rawContent, &parsed); err == nil {
		card = &parsed
	} else {
		card.Text = string(rawContent)
	}

	// Config-level overrides always win.
	if w.title != nil {
		title, err := w.title.TryString(msg)
		if err != nil {
			return nil, err
		}
		card.Title = title
	}

	if w.themeColor != "" {
		card.ThemeColor = w.themeColor
	}

	return card, nil
}

func (w *teamsWebhookWriter) Close(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.client = nil
	return nil
}
