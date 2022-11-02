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

// consumerCmd represents the consumer command
var consumerCmd = &cobra.Command{
	Use:   "consumer",
	Short: "consumer the pulsar topic",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("consumer called")
		// "pulsar://192.168.2.102:6650"
		url := cmd.Flag("url").Value.String()
		topic := cmd.Flag("topic").Value.String()
		name := cmd.Flag("name").Value.String()
		unsubscribe, err := cmd.Flags().GetBool("unsubscribe")
		if err != nil {
			log.Fatalf("Could not instantiate Pulsar client: %v", err)
		}
		client, err := pulsar.NewClient(pulsar.ClientOptions{
			URL:               url,
			OperationTimeout:  30 * time.Second,
			ConnectionTimeout: 30 * time.Second,
		})
		if err != nil {
			log.Fatalf("Could not instantiate Pulsar client: %v", err)
		}

		defer client.Close()
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{

			Topic:            topic, // example
			SubscriptionName: name,  //example-sub
			// Type:             pulsar.Shared,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer consumer.Close()

		go func() {
			for {
				msg, err := consumer.Receive(context.Background())
				if err != nil {
					log.Fatal(err)
					if unsubscribe {
						if err := consumer.Unsubscribe(); err != nil {
							log.Fatal(err)
						}
					}

				}

				fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
					msg.ID(), string(msg.Payload()))

				consumer.Ack(msg)
			}
		}()
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		<-sigc
		fmt.Println("ctrl+c pressed")
		if unsubscribe {
			if err := consumer.Unsubscribe(); err != nil {
				log.Fatal(err)

			}
		}
		os.Exit(1)

	},
}

func init() {
	rootCmd.AddCommand(consumerCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// consumerCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// consumerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
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
		name = "example-sub"
	}
	consumerCmd.Flags().StringP("url", "u", url, "指定pulsar broker 地址 或 proxy 地址 default: pulsar://localhost:6650")
	consumerCmd.Flags().StringP("topic", "t", topic, "指定消费的topic default: example(persistent://public/default/example)")
	consumerCmd.Flags().StringP("name", "n", name, "订阅名称 default: example-sub")
	consumerCmd.Flags().BoolP("unsubscribe", "s", false, "程序退出时是否取消订阅 default: false")

}
