package processed

import (
	"context"
	"os"
	"testing"
	"time"
)

// Cần Redis thật: REDIS_TEST_ADDR (và REDIS_TEST_PASSWORD nếu có). Không đặt thì bỏ qua.
func testStore(t *testing.T, ttl time.Duration) Store {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR not set")
	}
	return New(addr, os.Getenv("REDIS_TEST_PASSWORD"), "test:nlp:done:"+t.Name()+":", ttl)
}

func TestStore_MarkDone(t *testing.T) {
	s := testStore(t, time.Minute)
	ctx := context.Background()
	if done, err := s.Done(ctx, "a"); err != nil || done {
		t.Fatalf("before mark: done=%v err=%v", done, err)
	}
	if err := s.MarkDone(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if done, err := s.Done(ctx, "a"); err != nil || !done {
		t.Fatalf("after mark: done=%v err=%v", done, err)
	}
	if done, _ := s.Done(ctx, "b"); done {
		t.Fatal("other id marked")
	}
}

func TestStore_Expires(t *testing.T) {
	s := testStore(t, time.Second)
	ctx := context.Background()
	if err := s.MarkDone(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	if done, _ := s.Done(ctx, "a"); done {
		t.Fatal("key did not expire")
	}
}

func TestStore_Disabled(t *testing.T) {
	s := New("", "", "x:", time.Minute)
	if err := s.MarkDone(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if done, err := s.Done(context.Background(), "a"); done || err != nil {
		t.Fatalf("disabled store: done=%v err=%v", done, err)
	}
}

func TestStore_RedisDown(t *testing.T) {
	s := New("127.0.0.1:1", "", "x:", time.Minute)
	if _, err := s.Done(context.Background(), "a"); err == nil {
		t.Fatal("want error when redis is down")
	}
	if err := s.MarkDone(context.Background(), "a"); err == nil {
		t.Fatal("want error when redis is down")
	}
}
