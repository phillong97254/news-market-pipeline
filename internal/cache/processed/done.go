package processed

import (
	"context"
	"fmt"
)

func (i *impl) Done(ctx context.Context, id string) (bool, error) {
	n, err := i.client.Exists(ctx, i.key(id)).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists: %w", err)
	}
	return n == 1, nil
}

func (i *impl) MarkDone(ctx context.Context, id string) error {
	if err := i.client.Set(ctx, i.key(id), "1", i.ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}
