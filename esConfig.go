package elasticSearch

// elasticSearch配置结构
type EsConfig struct {
	Server          string
	Username        string
	Password        string
	ReplicasCount   int
	ShardsCount     int
	RefreshInterval int
	IndexFormat     string
}
