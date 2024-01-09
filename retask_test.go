package retask

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
)

func mockUUID() string {
	return "eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0"
}

func TestConsume(t *testing.T) {
	db, mock := redismock.NewClientMock()
	client, err := New(context.TODO(), "test", db)
	assert.NoError(t, err)

	type want struct {
		redis   string
		message *Message
	}

	tts := []struct {
		name string
		uuid string
		want want
	}{
		{
			name: "enqueue",
			uuid: "eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0",
			want: want{
				message: &Message{
					Data: "test",
					URN:  "eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0",
					raw:  "{\"_data\":\"test\",\"urn\":\"eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0\"}",
				},
				redis: "{\"_data\":\"test\",\"urn\":\"eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0\"}",
			},
		},
	}

	for _, tt := range tts {
		t.Run(tt.name, func(t *testing.T) {
			client.uuidFunc = mockUUID
			mock.ExpectLPush("retaskqueue-test", tt.want.redis).SetVal(1)
			_, err := client.Enqueue(context.Background(), []byte("test"))
			assert.NoError(t, err)

			mock.ExpectBRPop(0, "retaskqueue-test").SetVal([]string{"retaskqueue-test", tt.want.redis})
			got, err := client.Wait(context.Background())
			assert.Equal(t, tt.want.message, got)
		})
	}
}

func TestPopulateMessage(t *testing.T) {
	tts := []struct {
		name string
		got  string
		want *Message
	}{
		{
			name: "fi",
			got:  "{\"_data\":\"test\",\"urn\":\"eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0\"}",
			want: &Message{
				Data: "test",
				URN:  "eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0",
				raw:  "{\"_data\":\"test\",\"urn\":\"eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0\"}",
			},
		},
	}

	for _, tt := range tts {
		t.Run(tt.name, func(t *testing.T) {
			got, err := populateMessage(context.TODO(), tt.got)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWorker(t *testing.T) {
	db, mock := redismock.NewClientMock()
	client, err := New(context.TODO(), "test", db)
	assert.NoError(t, err)

	type want struct {
		redis   string
		message *Message
	}

	tts := []struct {
		name string
		uuid string
		want want
	}{
		{
			name: "enqueue",
			uuid: "eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0",
			want: want{
				message: &Message{
					Data: "test",
					URN:  "eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0",
					raw:  "{\"_data\":\"test\",\"urn\":\"eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0\"}",
				},
				redis: "{\"_data\":\"test\",\"urn\":\"eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0\"}",
			},
		},
	}

	for _, tt := range tts {
		t.Run(tt.name, func(t *testing.T) {
			client.uuidFunc = mockUUID
			go func() {
				for {
					time.Sleep(1 * time.Second)
					t.Logf("looking for message")
					mock.ExpectBRPop(0, "retaskqueue-test").SetVal([]string{"retaskqueue-test", tt.want.redis})
					task, err := client.Wait(context.TODO())
					assert.NoError(t, err)

					t.Logf("Received this task: %s with URN: %s", task.Data, task.URN)
				}
			}()
			t.Logf("sleep 3 seconds before adding message")
			time.Sleep(3 * time.Second)

			mock.ExpectLPush("retaskqueue-test", tt.want.redis).SetVal(1)
			t.Logf("enqueue one message")
			_, err := client.Enqueue(context.Background(), []byte("test"))
			assert.NoError(t, err)
		})
	}
}
