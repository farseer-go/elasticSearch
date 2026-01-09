package elasticSearch

import (
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/core"
	"github.com/olivere/elastic/v7"
)

// IInternalContext 内部上下文
type IInternalContext interface {
	Original() *elastic.Client
}

// internalContext 数据库上下文
type internalContext struct {
	esConfig *EsConfig // 数据库配置
}

// Original 返回原生的对象
func (receiver *internalContext) Original() *elastic.Client {
	client, _ := open(receiver.esConfig)
	return client
}

// RegisterInternalContext 注册内部上下文
func RegisterInternalContext(name string, configString string) {
	config := configure.ParseString[EsConfig](configString)
	if config.Server == "" {
		panic("[farseer.yaml]ElasticSearch." + name + ".Server，配置不正确")
	}
	if config.RefreshInterval == 0 {
		config.RefreshInterval = 1
	}
	if config.ReplicasCount == 0 {
		config.ReplicasCount = 1
	}
	if config.ShardsCount == 0 {
		config.ShardsCount = 1
	}

	// 注册上下文
	container.RegisterInstance[IInternalContext](&internalContext{esConfig: &config}, name)

	// 注册健康检查
	container.RegisterInstance[core.IHealthCheck](&healthCheck{name: name}, "es_"+name)
}
