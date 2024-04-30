package retask

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
)

const mockUUID = "eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0"

func TestConsume(t *testing.T) {
	testUUID = mockUUID
	db, mock := redismock.NewClientMock()
	client, err := New(context.TODO(), db)
	assert.NoError(t, err)

	type want struct {
		redis string
		task  *Task
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
				task: &Task{
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
			mock.MatchExpectationsInOrder(false)
			mock.ExpectBRPop(0, "retaskqueue-test").SetVal([]string{"retaskqueue-test", tt.want.redis})
			mock.ExpectLPush("retaskqueue-test", tt.want.redis).SetVal(1)
			testQueue := client.NewQueue(context.Background(), "test")

			_, err := testQueue.Enqueue(context.Background(), []byte("test"))
			assert.NoError(t, err)

			time.Sleep(1 * time.Second)

			got, err := testQueue.Wait(context.Background())
			fmt.Println("wait error", err)
			assert.Equal(t, tt.want.task.Data, got.Data)
		})
	}
}

func TestPopulateTask(t *testing.T) {
	testUUID = mockUUID
	tts := []struct {
		name string
		got  string
		want *Task
	}{
		{
			name: "fi",
			got:  "{\"_data\":\"test\",\"urn\":\"eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0\"}",
			want: &Task{
				Data: "test",
				URN:  "eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0",
				raw:  "{\"_data\":\"test\",\"urn\":\"eb2b0322-b0b4-4caf-8bb5-f2fee8c77ab0\"}",
			},
		},
	}

	for _, tt := range tts {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeTask(context.TODO(), tt.got)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWorker(t *testing.T) {
	testUUID = mockUUID
	db, mock := redismock.NewClientMock()
	client, err := New(context.TODO(), db)
	assert.NoError(t, err)

	type want struct {
		redis string
		task  *Task
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
				task: &Task{
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
			testQueue := client.NewQueue(context.Background(), "test")

			mock.MatchExpectationsInOrder(false)
			mock.ExpectLPush("retaskqueue-test", tt.want.redis).SetVal(1)
			mock.ExpectBRPop(0*time.Microsecond, "retaskqueue-test").SetVal([]string{"retaskqueue-test", tt.want.redis})

			t.Logf("enqueue one task")
			_, err := testQueue.Enqueue(context.Background(), []byte("test"))
			assert.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for {
					time.Sleep(1 * time.Second)
					t.Logf("looking for a task...")
					task, err := testQueue.Wait(context.TODO())
					assert.NoError(t, err)

					t.Logf("Received this task: %s with URN: %s", task.Data, task.URN)

					time.Sleep(1 * time.Second)
					wg.Done()
				}
			}()

			wg.Wait()

			mock.ClearExpect()
		})
	}
}
