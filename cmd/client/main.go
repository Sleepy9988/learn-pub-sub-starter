package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	const connection_str = "amqp://guest:guest@localhost:5672/"

	rabbit_conn, err := amqp.Dial(connection_str)
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
		return
	}
	defer rabbit_conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	publishCh, err := rabbit_conn.Channel()
	if err != nil {
		log.Fatalf("Could not create channel %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Could not obtain username: %v", err)
	}

	queueName := routing.PauseKey + "." + username

	/*
		_, queue, err := pubsub.DeclareAndBind(
			rabbit_conn,
			routing.ExchangePerilDirect,
			queueName,
			routing.PauseKey,
			pubsub.SimpleQueueTransient,
		)
		if err != nil {
			log.Fatalf("Could not subscribe to pause: %v", err)
		}
		fmt.Printf("Queue %v declared and bound!\n", queue.Name)
	*/
	gamestate := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		rabbit_conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gamestate),
	)
	if err != nil {
		log.Fatalf("Could not subscribe to pause: %v", err)
	}

	err = pubsub.SubscribeJSON(
		rabbit_conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gamestate.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gamestate, publishCh),
	)
	if err != nil {
		log.Fatalf("could not subscribe to army moves: %v", err)
	}

	err = pubsub.SubscribeJSON(
		rabbit_conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable,
		handlerWarMessages(gamestate, publishCh),
	)

	if err != nil {
		log.Fatalf("could not subscribe to war declarations: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err = gamestate.CommandSpawn(words)
			if err != nil {
				log.Println(err)
				continue
			}
		case "move":
			mv, err := gamestate.CommandMove(words)
			if err != nil {
				log.Println(err)
				continue
			}
			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+mv.Player.Username, mv)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(mv.Units), mv.ToLocation)
		case "status":
			gamestate.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			if len(words) < 2 {
				fmt.Println("usage: spam <n>")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Printf("error: %s is not a valid number\n", words[1])
				continue
			}
			for range n {
				msg := gamelogic.GetMaliciousLog()
				err = PublishGameLog(publishCh, gamestate.GetUsername(), msg)

				if err != nil {
					fmt.Printf("error publishing malicious log: %s\n", err)
				}
			}
			fmt.Printf("Published %v malicious logs\n", n)

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("Command unknown")
		}

	}

	/*
		// wait for ctrl + c
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt)
		<-signalChan
		fmt.Println("RabbitMQ connection closed.")
	*/
}
