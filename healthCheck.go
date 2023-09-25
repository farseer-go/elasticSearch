package elasticSearch

import (
	"github.com/farseer-go/fs"
	"github.com/farseer-go/fs/flog"
)

type healthCheck struct {
	name string
	IInternalContext
}

func (c *healthCheck) Check() (string, error) {
	InitContext(c, c.name)
	healthResponse, err := c.Original().CatHealth().Do(fs.Context)
	flog.ErrorIfExists(err)
	for _, catHealthResponseRow := range healthResponse {
		if catHealthResponseRow.Status != "green" {
			flog.Warningf("es name=%s，%s 有节点不健康：%s", c.name, catHealthResponseRow.Cluster, catHealthResponseRow.Status)
		}
	}
	return "ElasticSearch." + c.name, err
}
