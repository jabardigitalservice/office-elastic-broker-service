package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"syscall"
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

	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGKILL)

	ctx, cancel := context.WithCancel(context.Background())

	// go routine for getting signals asynchronously
	go func() {
		sig := <-signals
		log.Println("Got signal: ", sig)
		cancel()
	}()

	// Need refactor
	if viper.GetString("KAFKA_HOST") == "" {
		log.Fatal("Config/environment variable KAFKA_HOST not defined.")
	}

	if viper.GetString("KAFKA_CONSUMER_GROUP_NAME") == "" {
		log.Fatal("Config/environment variable KAFKA_CONSUMER_GROUP_NAME not defined.")
	}

	if viper.GetString("ELASTICSEARCH_DSN") == "" {
		log.Fatal("Config/environment variable ELASTICSEARCH_DSN not defined.")
	}

	if viper.GetString("ELASTICSEARCH_API_KEY") == "" {
		log.Fatal("Config/environment variable ELASTICSEARCH_API_KEY not defined.")
	}

	if viper.GetString("ELASTICSEARCH_INDEX_NAME") == "" {
		log.Fatal("Config/environment variable ELASTICSEARCH_INDEX_NAME not defined.")
	}

	cfg := Config{
		KafkaHost:              viper.GetString("KAFKA_HOST"),
		KafkaConsumerGroupName: viper.GetString("KAFKA_CONSUMER_GROUP_NAME"),
		ElasticsearchDSN:       viper.GetString("ELASTICSEARCH_DSN"),
		ElasticsearchApiKey:    viper.GetString("ELASTICSEARCH_API_KEY"),
		ElasticsearchIndexName: viper.GetString("ELASTICSEARCH_INDEX_NAME"),
	}

	log.Printf("Initializing Kafka client (%s)", cfg.KafkaHost)
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{cfg.KafkaHost},
		GroupTopics: []string{"analytic_event", "esign_activity"},
		GroupID:     cfg.KafkaConsumerGroupName,
	})

	defer func() {
		err := kafkaReader.Close()
		if err != nil {
			log.Println("Error closing Kafka consumer: ", err)
			return
		}
		log.Println("Kafka Consumer closed")
	}()

	log.Printf("Initializing Elasticsearch client (%s)", cfg.ElasticsearchDSN)
	elasticConfig := elasticsearch.Config{
		Addresses: []string{
			cfg.ElasticsearchDSN,
		},
		APIKey: cfg.ElasticsearchApiKey,
	}
	es, err := elasticsearch.NewClient(elasticConfig)
	if err != nil {
		// @TODO should better signal/error handling
		_ = kafkaReader.Close()
		log.Fatalf("Error creating the Elasticsearch client (%s)", err)
	}

	_, err = es.Ping()
	if err != nil {
		// @TODO should better signal/error handling
		_ = kafkaReader.Close()
		log.Fatalf("Error connecting to Elasticsearch server (%s)", err)
	}

	log.Printf("Connected to Elasticsearch server.")

	log.Printf("Listening from Kafka... (Topics: %s, Consumer Group: %s)", kafkaReader.Config().GroupTopics, kafkaReader.Config().GroupID)
	for {
		m, err := kafkaReader.FetchMessage(ctx) // without auto-commit
		if err != nil {
			// @TODO should better signal/error handling
			_ = kafkaReader.Close()
			log.Fatalf("Error fetching message from Kafka (%s)", err)
		}

		log.Printf(
			"Received message from Kafka (Created: %s, Topic: %s, Partition: %v, Offset: %v, Key: %s, Value: %s)",
			m.Time, m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value),
		)

		var payloadMap map[string]interface{}
		if err := json.Unmarshal(m.Value, &payloadMap); err != nil {
			log.Fatalf("Error unmarshal JSON string to Map: %s", err)
		}

		payloadMap["kafka_topic"] = m.Topic
		payloadMap["kafka_partition"] = m.Partition
		payloadMap["kafka_offset"] = m.Offset
		payloadMap["kafka_timestamp"] = m.Time

		var payloadByte []byte
		payloadByte, err = json.Marshal(payloadMap)
		if err != nil {
			log.Fatalf("Error marshal from Map to Byte JSON String: (%s)", err)
		}

		if err := createDocument(es, payloadByte); err != nil {
			// @TODO should better signal/error handling
			_ = kafkaReader.Close()
			log.Fatalf("Elasticsearch error: %s", err)
		}

		if err := kafkaReader.CommitMessages(ctx, m); err != nil {
			// @TODO should better signal/error handling
			_ = kafkaReader.Close()
			log.Fatalf("Failed commit messages to Kafka server (%s)", err)
		}
	}
}

func createDocument(es *elasticsearch.Client, payload []byte) error {
	timeZone := time.FixedZone("Asia/Jakarta", 7*3600)
	currentDatetime := time.Now().In(timeZone)
	indexNamePrefix := viper.GetString("ELASTICSEARCH_INDEX_NAME")
	indexName := fmt.Sprintf("%s-%s", indexNamePrefix, currentDatetime.Format("2006.01"))

	// Set up the request object.
	req := esapi.IndexRequest{
		Index:   indexName,
		Body:    bytes.NewReader(payload),
		Refresh: "true",
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), es)
	if err != nil {
		return fmt.Errorf("elasticsearch error getting response (Index: %s, %s, %s)", indexName, payload, err)
	}

	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("cannot create document (index: %s, %s, %s)", indexName, payload, res.Status())
	}

	log.Printf("Elasticsearch successfully create document (index: %s, %s)", indexName, payload)

	return nil
}
