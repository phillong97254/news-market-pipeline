package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

func (i *impl) Classify(ctx context.Context, input ClassifyInput) (domain.NewsAnalysis, error) {
	start := i.now()
	user := "Title: " + input.Title
	if input.Summary != "" {
		user += "\nSummary: " + input.Summary
	}
	body, err := json.Marshal(chatRequest{
		Model: i.model, Stream: false, KeepAlive: "30m", Format: outputSchema(),
		Options:  options(input.Attempt),
		Messages: []chatMessage{{Role: "system", Content: systemPrompt}, {Role: "user", Content: user}},
	})
	if err != nil {
		return domain.NewsAnalysis{}, fmt.Errorf("encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, i.baseURL+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return domain.NewsAnalysis{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := i.http.Do(req)
	if err != nil {
		return domain.NewsAnalysis{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return domain.NewsAnalysis{}, fmt.Errorf("%w: read body: %v", ErrUnavailable, err)
	}
	if resp.StatusCode >= 500 {
		return domain.NewsAnalysis{}, fmt.Errorf("%w: status %d: %s", ErrUnavailable, resp.StatusCode, raw)
	}
	var cr chatResponse
	if err := json.Unmarshal(raw, &cr); err != nil {
		return domain.NewsAnalysis{}, fmt.Errorf("decode response: %w", err)
	}
	if resp.StatusCode != http.StatusOK || cr.Error != "" {
		// 4xx: sai tên model, request sai... thử lại không giúp gì, để service quyết định.
		return domain.NewsAnalysis{}, fmt.Errorf("ollama status %d: %s", resp.StatusCode, cr.Error)
	}

	var l labels
	if err := json.Unmarshal([]byte(cr.Message.Content), &l); err != nil {
		return domain.NewsAnalysis{}, fmt.Errorf("%w: %v", ErrBadOutput, err)
	}
	a, err := normalize(l)
	if err != nil {
		return domain.NewsAnalysis{}, err
	}
	a.Model = i.model
	a.ProcessedAt = i.now().UTC()
	a.LatencyMS = i.now().Sub(start).Milliseconds()
	return a, nil
}

func options(attempt int) chatOptions {
	o := chatOptions{Temperature: 0, NumCtx: 2048, Seed: 1 + attempt, NumPredict: maxOutputTokens}
	if attempt > 0 {
		o.Temperature = 0.3
	}
	return o
}

// normalize ép nhãn về miền hợp lệ: schema của Ollama giới hạn enum nhưng không giới hạn khoảng số.
func normalize(l labels) (domain.NewsAnalysis, error) {
	if !slices.Contains(domain.Categories, l.Category) {
		return domain.NewsAnalysis{}, fmt.Errorf("%w: category %q", ErrBadOutput, l.Category)
	}
	a := domain.NewsAnalysis{
		TitleEN:   strings.TrimSpace(l.TitleEN),
		Category:  l.Category,
		Relevant:  l.Relevant,
		Sentiment: max(-1, min(1, l.Sentiment)),
		Impact:    max(0, min(3, l.Impact)),
		Assets:    []string{},
	}
	if !a.Relevant {
		a.Impact = 0
		return a, nil
	}
	for _, s := range l.Assets {
		if slices.Contains(domain.Assets, s) && !slices.Contains(a.Assets, s) {
			a.Assets = append(a.Assets, s)
		}
	}
	slices.Sort(a.Assets)
	return a, nil
}
