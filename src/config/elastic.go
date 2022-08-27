package config

import "github.com/elastic/go-elasticsearch/v8"

func ElasticConfig(config Config) (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			config.ElasticsearchDSN,
		},
		Username: config.ElasticsearchUsername,
		Password: config.ElasticsearchPassword,
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return es, nil
}
