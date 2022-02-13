package main

import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/nats-io/nats.go"
)

const (
	streamName     = "ORDERS"
	streamSubjects = "ORDERS.*"
	subjectName    = "ORDERS.created"
)

func main() {
	// Connect to NATS
	nc, _ := nats.Connect(nats.DefaultURL)
	// Creates JetStreamContext
	js, err := nc.JetStream()
	checkErr(err)
	// creates stream
	err = createStream(js)
	checkErr(err)
	log.Print("done creating the stream")
	//create orders by publishing messages
	err = createOrder(js)
	checkErr(err)
}

func createOrder(js nats.JetStreamContext) error {
	var order Order
	for i := 1; i <= 10000000000; i++ {
		order = Order{
			OrderID:    i,
			CustomerID: "Custu-" + strconv.Itoa(i),
			Status:     "created",
		}
		orderJSON, _ := json.Marshal(order)
		_, err := js.Publish(subjectName, orderJSON)
		if err != nil {
			log.Print("Error while publish")
			return err
		}
		log.Printf("Order with OrderID: %d has been published\n", i)
	}
	return nil
}

func createStream(js nats.JetStreamContext) error {
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		log.Println(err)
	}
	if stream == nil {
		log.Printf("creating stream %q and subject %q", streamName, streamSubjects)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubjects},
		})
		if err != nil {
			return nil
		}
	}
	return nil
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
