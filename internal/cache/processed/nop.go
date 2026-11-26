package processed

import "context"

type nop struct{}

func (nop) Done(context.Context, string) (bool, error) { return false, nil }
func (nop) MarkDone(context.Context, string) error     { return nil }
