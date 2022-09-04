package elasticSearch

// elasticSearch配置结构
type elasticConfig struct {
	Server          string
	Username        string
	Password        string
	ReplicasCount   int
	ShardsCount     int
	RefreshInterval int
	IndexFormat     string
}
