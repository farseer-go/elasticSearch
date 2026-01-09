package elasticSearch

import (
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/core"
	"github.com/farseer-go/fs/modules"
)

type Module struct {
}

func (module Module) DependsModule() []modules.FarseerModule {
	return nil
}

func (module Module) PreInitialize() {
	// 注册包级别的连接检查器（默认实现）
	container.Register(func() core.IConnectionChecker { return &connectionChecker{} }, "elasticSearch")
}

func (module Module) Initialize() {
	nodes := configure.GetSubNodes("ElasticSearch")
	for name, val := range nodes {
		configString := val.(string)
		if configString == "" {
			panic("[farseer.yaml]ElasticSearch." + name + "，配置不正确")
		}
		RegisterInternalContext(name, configString)
	}
}
