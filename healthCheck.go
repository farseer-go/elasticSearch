package elasticSearch

import "github.com/farseer-go/fs/flog"

type healthCheck struct {
	name string
}

func (c *healthCheck) Check() (string, error) {
	esContext := initConfig(c.name)
	esClient := open(esContext.esConfig)
	_, err := esClient.ElasticsearchVersion(esContext.esConfig.Server)
	flog.ErrorIfExists(err)
	return "ElasticSearch." + c.name, err
}
