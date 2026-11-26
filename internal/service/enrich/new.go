package enrich

import (
	"log/slog"

	"github.com/phillong97254/news-market-pipeline/internal/cache/processed"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/ollama"
	"github.com/phillong97254/news-market-pipeline/internal/publisher/kafka"
)

// Số lần hỏi lại model khi nó trả nội dung sai schema, trước khi bỏ bản tin.
const maxBadOutput = 3

type impl struct {
	logger *slog.Logger
	llm    ollama.Gateway
	pub    kafka.Publisher
	topic  string
	done   processed.Store
}

func New(logger *slog.Logger, llm ollama.Gateway, pub kafka.Publisher, topic string, done processed.Store) Service {
	return &impl{logger: logger.With("service", "enrich"), llm: llm, pub: pub, topic: topic, done: done}
}
