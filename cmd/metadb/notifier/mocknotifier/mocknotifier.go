package mocknotifier

import (
	"context"
)

// mockNotifier used when notifier is not specify

func NewMock() *mockNotifier {
	return &mockNotifier{}
}

type mockNotifier struct {
}

func (n *mockNotifier) Notify(ctx context.Context, message string) error {
	return nil
}
