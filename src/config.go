package main

type Config struct {
	KafkaHost              string
	KafkaConsumerGroupName string
	ElasticsearchDSN       string
	ElasticsearchUsername  string
	ElasticsearchPassword  string
	ElasticsearchIndexName string
}
