package redis

import (
	"context"
	"fmt"
	"strings"

	"github.com/TicketsBot-cloud/import-sync/internal/config"
)

func (c *RedisClient) MarkGuildProcessing(ctx context.Context, guildId uint64) (bool, error) {
	key := fmt.Sprintf("tickets:importsync:%s:%d", strings.ToLower(config.Conf.Daemon.Type), guildId)

	res, err := c.SetNX(ctx, key, "1", 0).Result()
	if err != nil {
		return false, err
	}

	return res, nil
}

func (c *RedisClient) IsGuildProcessing(ctx context.Context, guildId uint64) (bool, error) {
	key := fmt.Sprintf("tickets:importsync:%s:%d", strings.ToLower(config.Conf.Daemon.Type), guildId)

	res, err := c.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}

	return res == 1, nil
}

func (c *RedisClient) MarkGuildProcessed(ctx context.Context, guildId uint64) error {
	key := fmt.Sprintf("tickets:importsync:%s:%d", strings.ToLower(config.Conf.Daemon.Type), guildId)

	return c.Del(ctx, key).Err()
}
