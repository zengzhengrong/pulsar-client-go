/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/spf13/cobra"
)

func send(p pulsar.Producer) {
	_, err := p.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte("hello"),
	})
	fmt.Println("Published message")
	if err != nil {
		log.Fatal(err)
	}
}

// producerCmd represents the producer command
var producerCmd = &cobra.Command{
	Use:   "producer",
	Short: "Send messahe to broker",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("producer called")
		url := cmd.Flag("url").Value.String()
		topic := cmd.Flag("topic").Value.String()
		name := cmd.Flag("name").Value.String()
		client, err := pulsar.NewClient(pulsar.ClientOptions{
			URL:               url,
			OperationTimeout:  30 * time.Second,
			ConnectionTimeout: 30 * time.Second,
		})
		if err != nil {
			log.Fatalf("Could not instantiate Pulsar client: %v", err)
		}

		defer client.Close()
		producer, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic,
			Name:  name,
		})

		if err != nil {
			log.Fatal(err)
		}
		ticker := time.NewTicker(5 * time.Second)
		go func() {

			for range ticker.C {
				send(producer)
			}

		}()

		defer producer.Close()

		if err != nil {
			fmt.Println("Failed to publish message", err)
		}

		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		<-sigc
		fmt.Println("ctrl+c pressed")
		os.Exit(1)
	},
}

func init() {
	rootCmd.AddCommand(producerCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// producerCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// producerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	url := os.Getenv("URL")
	if url == "" {
		url = "pulsar://localhost:6650"
	}
	topic := os.Getenv("TOPIC")
	if topic == "" {
		topic = "example"
	}
	name := os.Getenv("Name")
	if name == "" {
		name = "example-producer"
	}
	producerCmd.Flags().StringP("url", "u", url, "指定pulsar broker 地址 或 proxy 地址")
	producerCmd.Flags().StringP("topic", "t", topic, "指定发送的topic")
	producerCmd.Flags().StringP("name", "n", name, "producer 名称")
}
