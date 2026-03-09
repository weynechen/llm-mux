package providers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	codexauth "github.com/nghyane/llm-mux/internal/auth/codex"
	"github.com/nghyane/llm-mux/internal/config"
	log "github.com/nghyane/llm-mux/internal/logging"
	"github.com/nghyane/llm-mux/internal/misc"
	"github.com/nghyane/llm-mux/internal/provider"
	"github.com/nghyane/llm-mux/internal/registry"
	"github.com/nghyane/llm-mux/internal/runtime/executor"
	"github.com/nghyane/llm-mux/internal/runtime/executor/stream"
	"github.com/nghyane/llm-mux/internal/sseutil"
	"github.com/nghyane/llm-mux/internal/translator/ir"
	"github.com/nghyane/llm-mux/internal/translator/to_ir"
	"github.com/nghyane/llm-mux/internal/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/tiktoken-go/tokenizer"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type CodexExecutor struct {
	executor.BaseExecutor
}

func NewCodexExecutor(cfg *config.Config) *CodexExecutor {
	return &CodexExecutor{BaseExecutor: executor.BaseExecutor{Cfg: cfg}}
}

func (e *CodexExecutor) Identifier() string { return "codex" }

func (e *CodexExecutor) PrepareRequest(_ *http.Request, _ *provider.Auth) error { return nil }

func (e *CodexExecutor) Execute(ctx context.Context, auth *provider.Auth, req provider.Request, opts provider.Options) (resp provider.Response, err error) {
	apiKey, baseURL := codexCreds(auth)

	if baseURL == "" {
		baseURL = executor.CodexDefaultBaseURL
	}
	reporter := e.NewUsageReporter(ctx, e.Identifier(), req.Model, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	body, err := stream.TranslateToCodex(e.Cfg, from, req.Model, req.Payload, false, req.Metadata)
	if err != nil {
		log.Errorf("[CODEX DEBUG] TranslateToCodex failed: %v", err)
		return resp, err
	}

	body = e.setReasoningEffortByAlias(req.Model, body)
	body = e.ApplyPayloadConfig(req.Model, body)

	body, _ = sjson.SetBytes(body, "stream", true)
	body, _ = sjson.DeleteBytes(body, "previous_response_id")

	url := strings.TrimSuffix(baseURL, "/") + "/responses"
	log.Infof("[CODEX DEBUG] Execute request - model: %s, url: %s, body: %s", req.Model, url, string(body))

	httpReq, err := e.cacheHelper(ctx, from, url, req, body)
	if err != nil {
		log.Errorf("[CODEX DEBUG] cacheHelper failed: %v", err)
		return resp, err
	}
	applyCodexHeaders(httpReq, auth, apiKey)
	log.Infof("[CODEX DEBUG] Using apiKey length: %d, auth.ID: %s", len(apiKey), auth.ID)

	httpClient := e.NewHTTPClient(ctx, auth, 0)
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		log.Errorf("[CODEX DEBUG] HTTP request failed: %v", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return resp, executor.NewTimeoutError("request timed out")
		}
		return resp, err
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codex executor: close response body error: %v", errClose)
		}
	}()
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		result := executor.HandleHTTPError(httpResp, "codex executor")
		log.Errorf("[CODEX DEBUG] HTTP error status: %d, error: %v", httpResp.StatusCode, result.Error)
		return resp, result.Error
	}
	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		log.Errorf("[CODEX DEBUG] Read response body failed: %v", err)
		return resp, err
	}

	log.Infof("[CODEX DEBUG] Response data length: %d, content: %s", len(data), string(data))

	lines := bytes.Split(data, []byte("\n"))
	log.Infof("[CODEX DEBUG] Response split into %d lines", len(lines))

	for i, line := range lines {
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}

		line = bytes.TrimSpace(line[5:])
		eventType := gjson.GetBytes(line, "type").String()
		log.Infof("[CODEX DEBUG] Line %d event type: %s", i, eventType)

		if eventType != "response.completed" {
			continue
		}

		if detail := executor.ExtractUsageFromOpenAIResponse(line); detail != nil {
			reporter.Publish(ctx, detail)
		}

		fromFormat := provider.FromString("codex")
		log.Infof("[CODEX DEBUG] Translating response from codex to %s", from)
		translatedResp, err := stream.TranslateResponseNonStream(e.Cfg, fromFormat, from, line, req.Model)
		if err != nil {
			log.Errorf("[CODEX DEBUG] TranslateResponseNonStream failed: %v", err)
			return resp, err
		}
		log.Infof("[CODEX DEBUG] Translated response: %s", string(translatedResp))
		if translatedResp != nil {
			resp = provider.Response{Payload: translatedResp}
		} else {
			resp = provider.Response{Payload: line}
		}
		return resp, nil
	}
	log.Errorf("[CODEX DEBUG] No response.completed event found in %d lines", len(lines))
	err = executor.NewStatusError(408, "stream error: stream disconnected before completion: stream closed before response.completed", nil)
	return resp, err
}

func (e *CodexExecutor) ExecuteStream(ctx context.Context, auth *provider.Auth, req provider.Request, opts provider.Options) (streamChan <-chan provider.StreamChunk, err error) {
	apiKey, baseURL := codexCreds(auth)

	if baseURL == "" {
		baseURL = executor.CodexDefaultBaseURL
	}
	reporter := e.NewUsageReporter(ctx, e.Identifier(), req.Model, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	body, err := stream.TranslateToCodex(e.Cfg, from, req.Model, req.Payload, true, req.Metadata)
	if err != nil {
		return nil, err
	}

	body = e.setReasoningEffortByAlias(req.Model, body)
	body = e.ApplyPayloadConfig(req.Model, body)
	body, _ = sjson.SetBytes(body, "stream", true)
	body, _ = sjson.DeleteBytes(body, "previous_response_id")

	url := strings.TrimSuffix(baseURL, "/") + "/responses"
	httpReq, err := e.cacheHelper(ctx, from, url, req, body)
	if err != nil {
		return nil, err
	}
	applyCodexHeaders(httpReq, auth, apiKey)

	httpClient := e.NewHTTPClient(ctx, auth, 0)
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, executor.NewTimeoutError("request timed out")
		}
		return nil, err
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		data, readErr := io.ReadAll(httpResp.Body)
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codex executor: close response body error: %v", errClose)
		}
		if readErr != nil {
			return nil, readErr
		}
		log.Debugf("codex executor: error status: %d, body: %s", httpResp.StatusCode, executor.SummarizeErrorBody(httpResp.Header.Get("Content-Type"), data))
		return nil, executor.NewStatusError(httpResp.StatusCode, string(data), nil)
	}

	messageID := "resp-" + req.Model
	streamCtx := stream.NewStreamContext()
	translator := stream.NewStreamTranslator(e.Cfg, from, from.String(), req.Model, messageID, streamCtx)
	processor := &codexStreamProcessor{
		translator: translator,
	}

	preprocessor := func(line []byte) ([]byte, bool) {
		payload := sseutil.JSONPayload(line)
		if payload == nil {
			return nil, true
		}
		return payload, false
	}

	return stream.RunSSEStream(ctx, httpResp.Body, reporter, processor, stream.StreamConfig{
		ExecutorName:   "codex",
		Preprocessor:   preprocessor,
		SkipEmptyLines: true,
	}), nil
}

type codexStreamProcessor struct {
	translator *stream.StreamTranslator
}

func (p *codexStreamProcessor) ProcessLine(line []byte) ([][]byte, *ir.Usage, error) {
	events, err := to_ir.ParseOpenAIChunk(line)
	if err != nil {
		return nil, nil, err
	}
	if len(events) == 0 {
		return nil, nil, nil
	}

	result, err := p.translator.Translate(events)
	if err != nil {
		return nil, nil, err
	}
	return result.Chunks, result.Usage, nil
}

func (p *codexStreamProcessor) ProcessDone() ([][]byte, error) {
	return p.translator.Flush()
}

func (e *CodexExecutor) CountTokens(ctx context.Context, auth *provider.Auth, req provider.Request, opts provider.Options) (provider.Response, error) {
	from := opts.SourceFormat
	body, err := stream.TranslateToCodex(e.Cfg, from, req.Model, req.Payload, false, req.Metadata)
	if err != nil {
		return provider.Response{}, err
	}

	modelForCounting := req.Model

	body = e.setReasoningEffortByAlias(req.Model, body)

	body, _ = sjson.DeleteBytes(body, "previous_response_id")
	body, _ = sjson.SetBytes(body, "stream", false)

	enc, err := tokenizerForCodexModel(modelForCounting)
	if err != nil {
		return provider.Response{}, fmt.Errorf("codex executor: tokenizer init failed: %w", err)
	}

	count, err := countCodexInputTokens(enc, body)
	if err != nil {
		return provider.Response{}, fmt.Errorf("codex executor: token counting failed: %w", err)
	}

	usageJSON := fmt.Sprintf(`{"response":{"usage":{"input_tokens":%d,"output_tokens":0,"total_tokens":%d}}}`, count, count)
	return provider.Response{Payload: []byte(usageJSON)}, nil
}

type reasoningModelConfig struct {
	BaseModel string
	Efforts   map[string]string
}

var codexReasoningConfigs = []reasoningModelConfig{
	{
		BaseModel: "gpt-5",
		Efforts: map[string]string{
			"gpt-5":         "",
			"gpt-5-minimal": "minimal",
			"gpt-5-low":     "low",
			"gpt-5-medium":  "medium",
			"gpt-5-high":    "high",
		},
	},
	{
		BaseModel: "gpt-5-codex",
		Efforts: map[string]string{
			"gpt-5-codex":        "",
			"gpt-5-codex-low":    "low",
			"gpt-5-codex-medium": "medium",
			"gpt-5-codex-high":   "high",
		},
	},
	{
		BaseModel: "gpt-5-codex-mini",
		Efforts: map[string]string{
			"gpt-5-codex-mini":        "",
			"gpt-5-codex-mini-medium": "medium",
			"gpt-5-codex-mini-high":   "high",
		},
	},
	{
		BaseModel: "gpt-5.1",
		Efforts: map[string]string{
			"gpt-5.1":        "",
			"gpt-5.1-none":   "none",
			"gpt-5.1-low":    "low",
			"gpt-5.1-medium": "medium",
			"gpt-5.1-high":   "high",
		},
	},
	{
		BaseModel: "gpt-5.1-codex",
		Efforts: map[string]string{
			"gpt-5.1-codex":        "",
			"gpt-5.1-codex-low":    "low",
			"gpt-5.1-codex-medium": "medium",
			"gpt-5.1-codex-high":   "high",
		},
	},
	{
		BaseModel: "gpt-5.1-codex-mini",
		Efforts: map[string]string{
			"gpt-5.1-codex-mini":        "",
			"gpt-5.1-codex-mini-medium": "medium",
			"gpt-5.1-codex-mini-high":   "high",
		},
	},
	{
		BaseModel: "gpt-5.1-codex-max",
		Efforts: map[string]string{
			"gpt-5.1-codex-max":        "",
			"gpt-5.1-codex-max-low":    "low",
			"gpt-5.1-codex-max-medium": "medium",
			"gpt-5.1-codex-max-high":   "high",
			"gpt-5.1-codex-max-xhigh":  "xhigh",
		},
	},
	{
		BaseModel: "gpt-5.2",
		Efforts: map[string]string{
			"gpt-5.2": "",
		},
	},
}

func (e *CodexExecutor) setReasoningEffortByAlias(modelName string, payload []byte) []byte {
	for _, cfg := range codexReasoningConfigs {
		if effort, ok := cfg.Efforts[modelName]; ok {
			payload, _ = sjson.SetBytes(payload, "model", cfg.BaseModel)
			if effort != "" {
				payload, _ = sjson.SetBytes(payload, "reasoning.effort", effort)
			}
			return payload
		}
	}
	return payload
}

func tokenizerForCodexModel(model string) (tokenizer.Codec, error) {
	sanitized := strings.ToLower(strings.TrimSpace(model))
	switch {
	case sanitized == "":
		return tokenizer.Get(tokenizer.Cl100kBase)
	case strings.HasPrefix(sanitized, "gpt-5"):
		return tokenizer.ForModel(tokenizer.GPT5)
	case strings.HasPrefix(sanitized, "gpt-4.1"):
		return tokenizer.ForModel(tokenizer.GPT41)
	case strings.HasPrefix(sanitized, "gpt-4o"):
		return tokenizer.ForModel(tokenizer.GPT4o)
	case strings.HasPrefix(sanitized, "gpt-4"):
		return tokenizer.ForModel(tokenizer.GPT4)
	case strings.HasPrefix(sanitized, "gpt-3.5"), strings.HasPrefix(sanitized, "gpt-3"):
		return tokenizer.ForModel(tokenizer.GPT35Turbo)
	default:
		return tokenizer.Get(tokenizer.Cl100kBase)
	}
}

func countCodexInputTokens(enc tokenizer.Codec, body []byte) (int64, error) {
	if enc == nil {
		return 0, fmt.Errorf("encoder is nil")
	}
	if len(body) == 0 {
		return 0, nil
	}

	root := gjson.ParseBytes(body)
	var segments []string

	if inst := strings.TrimSpace(root.Get("instructions").String()); inst != "" {
		segments = append(segments, inst)
	}

	inputItems := root.Get("input")
	if inputItems.IsArray() {
		arr := inputItems.Array()
		for i := range arr {
			item := arr[i]
			switch item.Get("type").String() {
			case "message":
				content := item.Get("content")
				if content.IsArray() {
					parts := content.Array()
					for j := range parts {
						part := parts[j]
						if text := strings.TrimSpace(part.Get("text").String()); text != "" {
							segments = append(segments, text)
						}
					}
				}
			case "function_call":
				if name := strings.TrimSpace(item.Get("name").String()); name != "" {
					segments = append(segments, name)
				}
				if args := strings.TrimSpace(item.Get("arguments").String()); args != "" {
					segments = append(segments, args)
				}
			case "function_call_output":
				if out := strings.TrimSpace(item.Get("output").String()); out != "" {
					segments = append(segments, out)
				}
			default:
				if text := strings.TrimSpace(item.Get("text").String()); text != "" {
					segments = append(segments, text)
				}
			}
		}
	}

	tools := root.Get("tools")
	if tools.IsArray() {
		tarr := tools.Array()
		for i := range tarr {
			tool := tarr[i]
			if name := strings.TrimSpace(tool.Get("name").String()); name != "" {
				segments = append(segments, name)
			}
			if desc := strings.TrimSpace(tool.Get("description").String()); desc != "" {
				segments = append(segments, desc)
			}
			if params := tool.Get("parameters"); params.Exists() {
				val := params.Raw
				if params.Type == gjson.String {
					val = params.String()
				}
				if trimmed := strings.TrimSpace(val); trimmed != "" {
					segments = append(segments, trimmed)
				}
			}
		}
	}

	textFormat := root.Get("text.format")
	if textFormat.Exists() {
		if name := strings.TrimSpace(textFormat.Get("name").String()); name != "" {
			segments = append(segments, name)
		}
		if schema := textFormat.Get("schema"); schema.Exists() {
			val := schema.Raw
			if schema.Type == gjson.String {
				val = schema.String()
			}
			if trimmed := strings.TrimSpace(val); trimmed != "" {
				segments = append(segments, trimmed)
			}
		}
	}

	text := strings.Join(segments, "\n")
	if text == "" {
		return 0, nil
	}

	count, err := enc.Count(text)
	if err != nil {
		return 0, err
	}
	return int64(count), nil
}

func (e *CodexExecutor) Refresh(ctx context.Context, auth *provider.Auth) (*provider.Auth, error) {
	if auth == nil {
		return nil, executor.NewStatusError(500, "codex executor: auth is nil", nil)
	}
	var refreshToken string
	if auth.Metadata != nil {
		if v, ok := auth.Metadata["refresh_token"].(string); ok && v != "" {
			refreshToken = v
		}
	}
	if refreshToken == "" {
		return auth, nil
	}
	svc := codexauth.NewCodexAuth(e.Cfg)
	td, err := svc.RefreshTokensWithRetry(ctx, refreshToken, 3)
	if err != nil {
		return nil, err
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata["id_token"] = td.IDToken
	auth.Metadata["access_token"] = td.AccessToken
	if td.RefreshToken != "" {
		auth.Metadata["refresh_token"] = td.RefreshToken
	}
	if td.AccountID != "" {
		auth.Metadata["account_id"] = td.AccountID
	}
	auth.Metadata["email"] = td.Email
	auth.Metadata["expired"] = td.Expire
	auth.Metadata["type"] = "codex"
	now := time.Now().Format(time.RFC3339)
	auth.Metadata["last_refresh"] = now
	return auth, nil
}

func (e *CodexExecutor) cacheHelper(ctx context.Context, from provider.Format, url string, req provider.Request, rawJSON []byte) (*http.Request, error) {
	var cache CodexCache
	if from == "claude" {
		userIDResult := gjson.GetBytes(req.Payload, "metadata.user_id")
		if userIDResult.Exists() {
			var hasKey bool
			key := fmt.Sprintf("%s-%s", req.Model, userIDResult.String())
			if cache, hasKey = GetCodexCache(key); !hasKey {
				cache = CodexCache{
					ID:     uuid.New().String(),
					Expire: time.Now().Add(1 * time.Hour),
				}
				SetCodexCache(key, cache)
			}
		}
	} else if from == "openai-response" {
		promptCacheKey := gjson.GetBytes(req.Payload, "prompt_cache_key")
		if promptCacheKey.Exists() {
			cache.ID = promptCacheKey.String()
		}
	}

	rawJSON, _ = sjson.SetBytes(rawJSON, "prompt_cache_key", cache.ID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(rawJSON))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Conversation_id", cache.ID)
	httpReq.Header.Set("Session_id", cache.ID)
	return httpReq, nil
}

func applyCodexHeaders(r *http.Request, auth *provider.Auth, token string) {
	executor.SetCommonHeaders(r, "application/json")
	r.Header.Set("Authorization", "Bearer "+token)

	var ginHeaders http.Header
	if ginCtx, ok := r.Context().Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
		ginHeaders = ginCtx.Request.Header
	}

	misc.EnsureHeader(r.Header, ginHeaders, "Version", "0.21.0")
	misc.EnsureHeader(r.Header, ginHeaders, "Openai-Beta", "responses=experimental")
	misc.EnsureHeader(r.Header, ginHeaders, "Session_id", uuid.NewString())
	misc.EnsureHeader(r.Header, ginHeaders, "User-Agent", executor.DefaultCodexUserAgent)

	r.Header.Set("Accept", "text/event-stream")

	isAPIKey := false
	if auth != nil {
		if v := executor.AttrStringValue(auth.Attributes, "api_key"); v != "" {
			isAPIKey = true
		}
	}
	if !isAPIKey {
		r.Header.Set("Originator", "codex_cli_rs")
		if accountID := executor.MetaStringValue(auth.Metadata, "account_id"); accountID != "" {
			r.Header.Set("Chatgpt-Account-Id", accountID)
		}
	}
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(r, attrs)
}

func codexCreds(a *provider.Auth) (apiKey, baseURL string) {
	return executor.ExtractCreds(a, executor.CodexCredsConfig)
}

// FetchCodexModels fetches available models from the OpenAI API.
// It returns a list of models that are available for the given auth.
// Note: Codex uses a special base URL that doesn't support /models endpoint,
// so we use the standard OpenAI API endpoint instead.
func FetchCodexModels(ctx context.Context, auth *provider.Auth, cfg *config.Config) []*registry.ModelInfo {
	apiKey, _ := codexCreds(auth)
	if apiKey == "" {
		return nil
	}

	httpClient := executor.NewProxyAwareHTTPClient(ctx, cfg, auth, 0)

	// Use standard OpenAI API endpoint for models list
	// Codex base URL (chatgpt.com/backend-api/codex) doesn't support /models
	url := "https://api.openai.com/v1/models"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		log.Errorf("FetchCodexModels: failed to create request: %v", err)
		return nil
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	executor.SetCommonHeaders(req, "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Errorf("FetchCodexModels: failed to fetch models: %v", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Codex tokens often can't access standard OpenAI API, silently return nil to use static fallback
		log.Debugf("FetchCodexModels: unexpected status code: %d, using static fallback", resp.StatusCode)
		return nil
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("FetchCodexModels: failed to read response body: %v", err)
		return nil
	}

	// Parse OpenAI models response
	root := gjson.ParseBytes(data)
	modelsArr := root.Get("data").Array()
	if len(modelsArr) == 0 {
		log.Debugf("FetchCodexModels: no models found in response")
		return nil
	}

	var models []*registry.ModelInfo
	for _, m := range modelsArr {
		modelID := m.Get("id").String()
		if modelID == "" {
			continue
		}
		// Filter for GPT/Codex models only
		if !strings.HasPrefix(modelID, "gpt-") && !strings.HasPrefix(modelID, "codex-") {
			continue
		}

		model := &registry.ModelInfo{
			ID:       modelID,
			Object:   "model",
			OwnedBy:  m.Get("owned_by").String(),
			Type:     "codex",
			Created:  m.Get("created").Int(),
		}
		if model.OwnedBy == "" {
			model.OwnedBy = "openai"
		}
		models = append(models, model)
	}

	log.Debugf("FetchCodexModels: fetched %d models", len(models))
	return models
}
