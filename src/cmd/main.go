package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/jabardigitalservice/office-elastic-broker-service/cmd/server"
	"github.com/jabardigitalservice/office-elastic-broker-service/config"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"time"
)

func main() {
	viper.AutomaticEnv()

	log.SetOutput(os.Stdout)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		PadLevelText:  true,
	})

	log.Println("Starting application...")

	if err := run(); err != nil {
		log.Fatalln("Error:", err)
	}
}

func run() error {
	if viper.GetString("KAFKA_HOST") == "" {
		log.Fatal("Config/environment variable KAFKA_HOST not defined.")
	}

	if viper.GetString("KAFKA_CONSUMER_GROUP_NAME") == "" {
		log.Fatal("Config/environment variable KAFKA_CONSUMER_GROUP_NAME not defined.")
	}

	if viper.GetString("ELASTICSEARCH_DSN") == "" {
		log.Fatal("Config/environment variable ELASTICSEARCH_DSN not defined.")
	}

	if viper.GetString("ELASTICSEARCH_USERNAME") == "" {
		log.Fatal("Config/environment variable ELASTICSEARCH_USERNAME not defined.")
	}

	if viper.GetString("ELASTICSEARCH_PASSWORD") == "" {
		log.Fatal("Config/environment variable ELASTICSEARCH_PASSWORD not defined.")
	}

	if viper.GetString("ELASTICSEARCH_INDEX_NAME") == "" {
		log.Fatal("Config/environment variable ELASTICSEARCH_INDEX_NAME not defined.")
	}

	cfg := config.Config{
		KafkaHost:              viper.GetString("KAFKA_HOST"),
		KafkaConsumerGroupName: viper.GetString("KAFKA_CONSUMER_GROUP_NAME"),
		ElasticsearchDSN:       viper.GetString("ELASTICSEARCH_DSN"),
		ElasticsearchUsername:  viper.GetString("ELASTICSEARCH_USERNAME"),
		ElasticsearchPassword:  viper.GetString("ELASTICSEARCH_PASSWORD"),
		ElasticsearchIndexName: viper.GetString("ELASTICSEARCH_INDEX_NAME"),
	}

	log.Printf("Initializing Kafka client (%s)", cfg.KafkaHost)
	conn, err := kafka.DialLeader(context.Background(), "tcp", cfg.KafkaHost, "analytic_event", 0)
	if err != nil {
		log.Fatalf("Error connecting to Kafka server (%s)", err)
	}
	defer conn.Close()

	log.Printf("Connected to Kafka server.")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaHost},
		Topic:   "analytic_event",
		GroupID: cfg.KafkaConsumerGroupName,
		// StartOffset: kafka.LastOffset,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	log.Printf("Initializing Elasticsearch client (%s)", cfg.ElasticsearchDSN)
	es, err := config.ElasticConfig(cfg)
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client (%s)", err)
	}

	_, err = es.Ping()
	if err != nil {
		log.Fatalf("Error connecting to Elasticsearch server (%s)", err)
	}

	log.Printf("Connected to Elasticsearch server.")

	log.Printf("Listening from Kafka (Topic: %s, Group: %s)...", r.Config().Topic, r.Config().GroupID)
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("Error connecting to Kafka server (%s)", err)
		}

		log.Printf("Received message from Kafka %v/%v/%v: %s = %s", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		err = createDocument(es, string(m.Value))
		if err != nil {
			break
		}
	}

	if err := r.Close(); err != nil {
		log.Fatal("Failed to close reader:", err)
	}

	return server.Start(cfg)
}

func createDocument(es *elasticsearch.Client, payload string) error {
	timeZone := time.FixedZone("Asia/Jakarta", 7*3600)
	currentDatetime := time.Now().In(timeZone)
	indexNamePrefix := viper.GetString("ELASTICSEARCH_INDEX_NAME")
	indexName := fmt.Sprintf("%s-%s", indexNamePrefix, currentDatetime.Format("2006.01"))
	log.Printf("Elasticsearch index name: %s", indexName)

	// Set up the request object.
	req := esapi.IndexRequest{
		Index:   indexName,
		Body:    bytes.NewReader([]byte(payload)),
		Refresh: "true",
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Errorf("Elasticsearch error getting response (%s)", err)
		return err
	}

	log.Printf("Elasticsearch successfully create document %s", payload)

	defer res.Body.Close()

	if res.IsError() {
		log.Errorf("Elasticsearch error create document (%s)", res.Status())
		return err
	}

	return nil
}
