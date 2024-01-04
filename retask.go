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
	Data string `json:"_data"`
	URN  string `json:"urn"`
	raw  string
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
	//fmt.Println("msg", string(msg))
	return c.redisClient.LPush(ctx, c.queueName, string(msg)).Err()
}

func (c *Client) Wait(ctx context.Context) (*Message, error) {
	res, err := c.redisClient.BRPop(ctx, 0*time.Microsecond, c.queueName).Result()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, ErrNoResult
	}
	message, err := populateMessage(ctx, res[1])
	if err != nil {
		return nil, err
	}

	return message, err
}

func populateMessage(ctx context.Context, data string) (*Message, error) {
	m := &Message{
		raw: data,
	}
	if err := json.Unmarshal([]byte(data), m); err != nil {
		return nil, err
	}
	return m, nil
}
