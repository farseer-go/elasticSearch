package test

import (
	"github.com/farseer-go/elasticSearch"
	"github.com/farseer-go/fs"
	"github.com/farseer-go/fs/configure"
)

func init() {
	// 设置配置默认值，模拟配置文件
	configure.SetDefault("ElasticSearch.log_es", "Server=http://localhost:9200,Username=es,Password=123456,ReplicasCount=1,ShardsCount=1,RefreshInterval=5,IndexFormat=yyyy_MM")
	fs.Initialize[elasticSearch.Module]("test es")
}
