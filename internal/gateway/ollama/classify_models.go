package ollama

// Định dạng Ollama /api/chat.

type chatRequest struct {
	Model     string         `json:"model"`
	Stream    bool           `json:"stream"`
	KeepAlive string         `json:"keep_alive"`
	Format    map[string]any `json:"format"`
	Options   chatOptions    `json:"options"`
	Messages  []chatMessage  `json:"messages"`
}

type chatOptions struct {
	Temperature float64 `json:"temperature"`
	NumCtx      int     `json:"num_ctx"`
	Seed        int     `json:"seed"`
	NumPredict  int     `json:"num_predict"` // chặn trên số token sinh ra
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Message chatMessage `json:"message"`
	Error   string      `json:"error"`
}

// labels là nội dung JSON model sinh theo outputSchema.
type labels struct {
	TitleEN   string   `json:"title_en"`
	Category  string   `json:"category"`
	Relevant  bool     `json:"relevant"`
	Sentiment float64  `json:"sentiment"`
	Impact    int      `json:"impact"`
	Assets    []string `json:"assets"`
}
