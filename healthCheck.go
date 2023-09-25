package elasticSearch

import (
	"fmt"
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
			flog.Warningf("ElasticSearch name=%s，%s 有节点不健康：%s", c.name, catHealthResponseRow.Cluster, catHealthResponseRow.Status)
		}
	}
	do, err := c.Original().ClusterState().Do(fs.Context)
	return fmt.Sprintf("ElasticSearch.%s => %d个节点", c.name, len(do.Nodes)), err
}
