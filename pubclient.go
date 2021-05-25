package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/GaryBoone/GoStats/stats"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type PubClient struct {
	ID                   int
	BrokerURL            string
	BrokerUser           string
	BrokerPass           string
	BrokerClientIDPrefix string
	PubTopic             string
	MsgSize              int
	MsgCount             int
	PubQoS               byte
	KeepAlive            int
	Quiet                bool
	InsecureSkipVerify   bool
}

func (c *PubClient) run(res chan *PubResults) {
	generatedMsgs := make(chan *Message)
	publishedMsgs := make(chan *Message)
	runResults := new(PubResults)

	started := time.Now()
	// start generator
	go c.genMessages(generatedMsgs)
	// start publisher
	go c.pubMessages(generatedMsgs, publishedMsgs)

	runResults.ID = c.ID
	times := []float64{}
	for {
		m, more := <-publishedMsgs
		if more {
			if m.Error {
				log.Printf("PUBLISHER %v ERROR publishing message: %v: at %v\n", c.ID, m.Topic, m.Sent.Unix())
				runResults.Failures++
			} else {
				// log.Printf("Message published: %v: sent: %v delivered: %v flight time: %v\n", m.Topic, m.Sent, m.Delivered, m.Delivered.Sub(m.Sent))
				runResults.Successes++
				times = append(times, m.Delivered.Sub(m.Sent).Seconds()*1000) // in milliseconds
			}
		} else {
			// calculate results
			duration := time.Now().Sub(started)
			runResults.PubTimeMin = stats.StatsMin(times)
			runResults.PubTimeMax = stats.StatsMax(times)
			runResults.PubTimeMean = stats.StatsMean(times)
			runResults.PubTimeStd = stats.StatsSampleStandardDeviation(times)
			runResults.RunTime = duration.Seconds()
			runResults.PubsPerSec = float64(runResults.Successes) / duration.Seconds()

			// report results and exit
			res <- runResults
			return
		}
	}
}

func (c *PubClient) genMessages(generatedMsgs chan *Message) {
	for i := 0; i < c.MsgCount; i++ {
		generatedMsgs <- &Message{
			SeqNo: i,
			Topic: c.PubTopic,
			QoS:   c.PubQoS,
			//Payload: make([]byte, c.MsgSize),
		}
	}
	close(generatedMsgs)
	// log.Printf("PUBLISHER %v is done generating messages\n", c.ID)
}

func (c *PubClient) pubMessages(generatedMsgs, publishedMsgs chan *Message) {
	onConnected := func(client mqtt.Client) {
		for {
			m, more := <-generatedMsgs
			if more {
				m.Sent = time.Now()
				m.Payload = bytes.Join([][]byte{[]byte(strconv.FormatInt(m.Sent.UnixNano(), 10)), make([]byte, c.MsgSize)}, []byte("#@#"))
				token := client.Publish(m.Topic, m.QoS, false, m.Payload)

				if token.Wait() && token.Error() != nil {
					log.Printf("PUBLISHER %v Error sending message: %v\n", c.ID, token.Error())
					m.Error = true
				} else {
					m.Delivered = time.Now()
					m.Error = false
				}
				publishedMsgs <- m
			} else {
				if !c.Quiet {
					log.Printf("PUBLISHER %v had connected to the broker %v and done publishing for topic: %v\n", c.ID, c.BrokerURL, c.PubTopic)
				}
				close(publishedMsgs)
				return
			}
		}
	}

	ka, _ := time.ParseDuration(strconv.Itoa(c.KeepAlive) + "s")

	opts := mqtt.NewClientOptions().
		AddBroker(c.BrokerURL).
		SetClientID(c.BrokerClientIDPrefix + fmt.Sprintf("pub-%v-%v", time.Now().UnixNano(), c.ID)).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetOnConnectHandler(onConnected).
		SetKeepAlive(ka).
		SetTLSConfig(&tls.Config{
			InsecureSkipVerify: c.InsecureSkipVerify,
		}).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Printf("PUBLISHER %v lost connection to the broker: %v.\n", c.ID, reason.Error())
		})
	if c.BrokerUser != "" && c.BrokerPass != "" {
		opts.SetUsername(c.BrokerUser)
		opts.SetPassword(c.BrokerPass)
	}
	client := mqtt.NewClient(opts)
	token := client.Connect()

	if token.Wait() && token.Error() != nil {
		log.Printf("PUBLISHER %v had error connecting to the broker: %v\n", c.ID, token.Error())
	}
}
