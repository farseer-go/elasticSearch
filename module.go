package elasticSearch

import (
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/modules"
)

type Module struct {
}

func (module Module) DependsModule() []modules.FarseerModule {
	return nil
}

func (module Module) Initialize() {
	nodes := configure.GetSubNodes("ElasticSearch")
	for name, val := range nodes {
		configString := val.(string)
		if configString == "" {
			panic("[farseer.yaml]ElasticSearch." + name + "，没有正确配置")
		}
		RegisterInternalContext(name, configString)
	}
}
