package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	const connection_str = "amqp://guest:guest@localhost:5672/"

	rabbit_conn, err := amqp.Dial(connection_str)
	if err != nil {
		log.Fatalf("Could not establish connection %v", err)
		return
	}

	defer rabbit_conn.Close()

	publishCh, err := rabbit_conn.Channel()
	if err != nil {
		log.Fatalf("Could not create channel %v", err)
	}

	queueKey := routing.GameLogSlug + ".*"

	_, queue, err := pubsub.DeclareAndBind(
		rabbit_conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		queueKey,
		pubsub.SimpleQueueDurable,
	)
	if err != nil {
		log.Fatalf("Could not subscribe to queue: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		if words[0] == "pause" {
			fmt.Println("Publishing paused game state")
			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Printf("Could not publish time: %v", err)
			}
		} else if words[0] == "resume" {
			fmt.Println("Publishing resume game state ")
			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Printf("Could not publish time: %v", err)
			}
		} else if words[0] == "quit" {
			fmt.Println("Goodbye")
			return
		} else {
			fmt.Println("Unknown command")
		}
	}

	//fmt.Println("Peril game server connected to RabbitMQ!")
	/*
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt)
		<-signalChan
		fmt.Println("RabbitMQ connection closed.")
	*/

}
