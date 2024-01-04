package goretask

import (
	"context"
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
)

func TestConsume(t *testing.T) {
	db, mock := redismock.NewClientMock()
	client, err := New(context.TODO(), "test", db)
	assert.NoError(t, err)

	tts := []struct {
		name string
		uuid string
		want *Message
	}{
		{
			name: "enqueue",
			uuid: "eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0",
			want: &Message{
				QueueName: "retaskqueue-test",
				Data:      "{\"_data\":\"test\",\"urn\":\"eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0\"}",
			},
		},
	}

	for _, tt := range tts {
		t.Run(tt.name, func(t *testing.T) {
			urn = tt.uuid

			mock.ExpectLPush("retaskqueue-test", tt.want.Data).SetVal(1)
			err := client.Enqueue(context.Background(), []byte("test"))
			assert.NoError(t, err)

			mock.ExpectBRPop(0, "retaskqueue-test").SetVal([]string{"retaskqueue-test", tt.want.Data})
			got, err := client.Data(context.Background())
			assert.Equal(t, tt.want, got)
		})
	}
}
