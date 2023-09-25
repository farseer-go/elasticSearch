package elasticSearch

import "github.com/olivere/elastic/v7"

// 连接es
func open(esConfig *esConfig) *elastic.Client {
	es, err := elastic.NewClient(
		elastic.SetURL(esConfig.Server),
		elastic.SetBasicAuth(esConfig.Username, esConfig.Password),
		elastic.SetSniff(false), //非集群下，关闭嗅探机制
	)
	if err != nil {
		panic("elasticsearch初始化失败:" + err.Error())
	}
	return es
}
