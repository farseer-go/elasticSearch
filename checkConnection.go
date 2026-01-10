package elasticSearch

import (
	"context"
	"fmt"
	"time"

	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/core"
	"github.com/farseer-go/fs/trace"
)

// 确保实现了IConnectionChecker接口
var _ core.IConnectionChecker = (*connectionChecker)(nil)

type connectionChecker struct{}

// Check 检查连接字符串是否能成功连接到ElasticSearch
// 实现IConnectionChecker接口
func (c *connectionChecker) Check(configString string) (bool, error) {
	// 取消链路
	container.Resolve[trace.IManager]().Ignore()

	if configString == "" {
		return false, fmt.Errorf("连接字符串不能为空")
	}

	// 解析配置字符串
	config := configure.ParseString[EsConfig](configString)

	if config.Server == "" {
		return false, fmt.Errorf("Server配置不正确：%s", configString)
	}

	// 创建ElasticSearch客户端
	es, err := open(&config)
	if err != nil {
		return false, fmt.Errorf("连接ElasticSearch失败：%s", err.Error())
	}

	defer es.Stop()

	// Ping测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err = es.Ping(config.Server).Do(ctx)
	if err != nil {
		return false, fmt.Errorf("Ping ElasticSearch失败：%s", err.Error())
	}

	return true, nil
}

// CheckWithTimeout 带超时时间的连接检查
// 实现IConnectionChecker接口，参数为 time.Duration
// CheckWithTimeout 带超时时间的连接检查
// timeout 为0时使用默认的10秒超时
func (c *connectionChecker) CheckWithTimeout(configString string, timeout time.Duration) (bool, error) {
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	type result struct {
		success bool
		err     error
	}
	resultChan := make(chan result, 1)

	go func() {
		success, err := c.Check(configString)
		resultChan <- result{success: success, err: err}
	}()

	select {
	case <-ctx.Done():
		return false, fmt.Errorf("连接检查超时，超时时间：%v", timeout)
	case res := <-resultChan:
		return res.success, res.err
	}
}
