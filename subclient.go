package main

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/GaryBoone/GoStats/stats"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type SubClient struct {
	ID                   int
	BrokerURL            string
	BrokerUser           string
	BrokerPass           string
	BrokerClientIDPrefix string
	SubTopic             string
	SubQoS               byte
	KeepAlive            int
	Quiet                bool
	InsecureSkipVerify   bool
}

func (c *SubClient) run(res chan *SubResults, subDone chan bool, jobDone chan bool) {
	runResults := new(SubResults)
	runResults.ID = c.ID

	forwardLatency := []float64{}

	ka, _ := time.ParseDuration(strconv.Itoa(c.KeepAlive) + "s")

	opts := mqtt.NewClientOptions().
		AddBroker(c.BrokerURL).
		SetClientID(c.BrokerClientIDPrefix + fmt.Sprintf("sub-%v-%v", time.Now().UnixNano(), c.ID)).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetKeepAlive(ka).
		SetTLSConfig(&tls.Config{
			InsecureSkipVerify: c.InsecureSkipVerify,
		}).
		SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
			recvTime := time.Now().UnixNano()
			payload := msg.Payload()
			if len(payload) >= 16 {
				sendTime := int64(binary.LittleEndian.Uint64(payload[0:8]))
				seqNum := int64(binary.LittleEndian.Uint64(payload[8:16]))

				if runResults.Received != seqNum {
					log.Printf("SUBSCRIBER expected seqNum %v but received %v", runResults.Received, seqNum)
				}

				forwardLatency = append(forwardLatency, float64(recvTime-sendTime)/1000000) // in milliseconds
			}

			runResults.Received++
		}).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Printf("SUBSCRIBER %v lost connection to the broker: %v.\n", c.ID, reason.Error())
		})
	if c.BrokerUser != "" && c.BrokerPass != "" {
		opts.SetUsername(c.BrokerUser)
		opts.SetPassword(c.BrokerPass)
	}
	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("SUBSCRIBER %v had error connecting to the broker: %v\n", c.ID, token.Error())
		return
	}

	if token := client.Subscribe(c.SubTopic, c.SubQoS, nil); token.Wait() && token.Error() != nil {
		log.Printf("SUBSCRIBER %v had error subscribe with topic: %v\n", c.ID, token.Error())
		return
	}

	if !c.Quiet {
		log.Printf("SUBSCRIBER %v had connected to the broker: %v and subscribed with topic: %v\n", c.ID, c.BrokerURL, c.SubTopic)
	}

	subDone <- true
	for {
		select {
		case <-jobDone:
			client.Disconnect(250)
			runResults.FwdLatencyMin = stats.StatsMin(forwardLatency)
			runResults.FwdLatencyMax = stats.StatsMax(forwardLatency)
			runResults.FwdLatencyMean = stats.StatsMean(forwardLatency)
			runResults.FwdLatencyStd = stats.StatsSampleStandardDeviation(forwardLatency)
			res <- runResults
			if !c.Quiet {
				log.Printf("SUBSCRIBER %v is done subscribe\n", c.ID)
			}
			return
		}
	}
}
