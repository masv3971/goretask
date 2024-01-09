package retask

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func newURN() string {
	return uuid.NewString()
}

// Message is a serialized representation of a message in the queue
type Message struct {
	Data string `json:"_data"`
	URN  string `json:"urn"`
	raw  string
}

// Client is a client for interacting with the queue
type Client struct {
	redisClient *redis.Client
	queueName   string
	uuidFunc    func() string
	currentURN  string
}

func (c *Client) setNewURN() {
	c.currentURN = c.uuidFunc()
}

// New creates a new client for interacting with the queue
func New(ctx context.Context, queueName string, redisClient *redis.Client) (*Client, error) {
	client := &Client{
		redisClient: redisClient,
		queueName:   fmt.Sprintf("retaskqueue-%s", queueName),
		uuidFunc:    newURN,
	}

	return client, nil
}

// Enqueue adds a new message to the queue
func (c *Client) Enqueue(ctx context.Context, data []byte) (string, error) {
	c.setNewURN()
	msg, err := c.wrap(data)
	if err != nil {
		return "", err
	}
	if err := c.redisClient.LPush(ctx, c.queueName, msg).Err(); err != nil {
		return "", err
	}
	return c.currentURN, nil
}

// Wait waits for a message to be available in the queue
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

func (c *Client) wrap(data []byte) (string, error) {
	wrapper := map[string]any{
		"_data": string(data),
		"urn":   c.currentURN,
	}
	msg, err := json.Marshal(wrapper)
	if err != nil {
		return "", err
	}
	return string(msg), nil
}
