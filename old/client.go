package old

import (
	"context"
	"github.com/farseer-go/elasticSearch"
	"github.com/farseer-go/fs/configure"
	"github.com/olivere/elastic/v7"
)

type IClient interface {
}

// Client es客户端结构
type Client struct {
	Index *esIndex
	Query *esQuery
	Doc   *esDoc
}

var ctx = context.Background()

// NewClient 初始化elastic
func NewClient(esName string) *Client {
	configString := configure.GetString("" + esName)
	if configString == "" {
		panic("[farseer.yaml]找不到相应的配置：" + esName)
	}
	elasticConfig := configure.ParseString[elasticSearch.EsConfig](configString)
	client, err := elastic.NewClient(
		elastic.SetURL(elasticConfig.Server),
		elastic.SetBasicAuth(elasticConfig.Username, elasticConfig.Password),
		elastic.SetSniff(false), //非集群下，关闭嗅探机制
	)
	if err != nil {
		panic("ElasticSearch 客户端创建失败：" + err.Error())
	}
	index := &esIndex{Es: client}
	query := &esQuery{Es: client}
	doc := &esDoc{Es: client}
	return &Client{Query: query, Index: index, Doc: doc}
}
