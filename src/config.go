package main

type Config struct {
	KafkaHost              string
	KafkaConsumerGroupName string
	ElasticsearchDSN       string
	ElasticsearchApiKey    string
	ElasticsearchIndexName string
}
