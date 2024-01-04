package goretask

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Message struct {
	QueueName string
	Data      string
}

type Client struct {
	redisClient *redis.Client
	queueName   string
}

func New(ctx context.Context, queueName string, redisClient *redis.Client) (*Client, error) {
	client := &Client{
		redisClient: redisClient,
		queueName:   fmt.Sprintf("retaskqueue-%s", queueName),
	}

	return client, nil
}

var urn = uuid.NewString()

// Enqueue adds a new message to the queue
func (c *Client) Enqueue(ctx context.Context, data []byte) error {
	wrapper := map[string]any{
		"_data": string(data),
		"urn":   urn,
	}
	msg, err := json.Marshal(wrapper)
	if err != nil {
		return err
	}
	fmt.Println("msg", string(msg))
	return c.redisClient.LPush(ctx, c.queueName, string(msg)).Err()
}

func (c *Client) Data(ctx context.Context) (*Message, error) {
	res, err := c.redisClient.BRPop(ctx, 0*time.Microsecond, c.queueName).Result()
	if err != nil {
		return nil, err
	}
	message := &Message{
		QueueName: c.queueName,
		Data:      res[1],
	}

	return message, err
}