package ollama

import "github.com/phillong97254/news-market-pipeline/internal/domain"

// Giữ nguyên system prompt giữa các lần gọi để Ollama dùng lại cache phần đầu prompt.
// Thứ tự trường trong schema = thứ tự model sinh: dịch tiêu đề trước rồi mới phân loại.
const systemPrompt = `You label news for a study of how political and macroeconomic news relates to crypto and stock prices.
Fill the fields in order:
- title_en: the title in English (translate if needed, otherwise copy).
- category: one of
  trade (tariffs, trade deals, export controls), monetary_policy (central banks, interest rates, inflation data),
  fiscal (budgets, taxes, government debt), election (elections, leadership changes),
  geopolitics (war, military, diplomacy, terrorism), sanctions, crypto_regulation (laws/agencies/court cases about crypto),
  corporate (single company news), other (anything else: crime, sports, culture, accidents, celebrities).
- relevant: true only if a market participant could plausibly react to this news. Crime, celebrity, sports, local accidents are not relevant.
- sentiment: from -1 (clearly bad for risk assets) to 1 (clearly good); 0 if neutral or unclear.
- impact: 0 none, 1 low, 2 medium, 3 high (only for major surprises: rate decisions, new tariffs on major economies, wars starting, big crypto bans/approvals).
- assets: markets directly exposed. Use [] when relevant is false or the link is unclear. Do not add crypto unless the news is about crypto or broad risk sentiment.`

func outputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"title_en":  map[string]any{"type": "string"},
			"category":  map[string]any{"type": "string", "enum": domain.Categories},
			"relevant":  map[string]any{"type": "boolean"},
			"sentiment": map[string]any{"type": "number"},
			"impact":    map[string]any{"type": "integer"},
			// maxItems bắt buộc: không có nó model có lúc lặp mãi 1 phần tử ("US_STOCKS", "US_STOCKS", ...).
			"assets": map[string]any{
				"type":     "array",
				"maxItems": 4,
				"items":    map[string]any{"type": "string", "enum": domain.Assets},
			},
		},
		"required": []string{"title_en", "category", "relevant", "sentiment", "impact", "assets"},
	}
}
