package elasticSearch

// esConfig 配置
type esConfig struct {
	Server          string
	Username        string
	Password        string
	ReplicasCount   int
	ShardsCount     int
	RefreshInterval int
	IndexFormat     string
}
