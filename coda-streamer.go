/*
# go web services to publish message to kafka
# go run coda-streaming.go
# ==============================================================================
*/

package main

// Import required package
import (
		"fmt"
		"github.com/gin-gonic/gin"
		"github.com/confluentinc/confluent-kafka-go/kafka"
		"encoding/json"
		"net/http"
)

// Parse Json request body 
type InputData struct {
    Content ContentData `json:"content"`
    Topic string `json:"topic"`
}

type ContentData struct {
    DataType  string `json:"data_type"`
    DataSource string  `json:"data_source"`
	LatLong  []float64 `json:"lat_long"`
	Temperature  float64 `json:"temperature"`
	Units  string `json:"units"`
}

func main() {
    
    // Define config value
	BROKER := "127.0.0.1"
	TOPIC := "coda"

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": BROKER})
	if (err != nil){
		fmt.Printf("Error ",err)
	}

	r := gin.Default()

	r.POST("/coda/v0.01/stream-data/", func(c *gin.Context) {

		var inputdata InputData
		if err := c.BindJSON(&inputdata); err != nil {
			fmt.Printf("Error --- ",err)
		}

		JsonData, err := json.Marshal(inputdata)
	    if err != nil {
	        panic (err)
	    }

    	JsonString := string(JsonData)
        
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &TOPIC, Partition: kafka.PartitionAny},
			Value:          []byte(JsonString),
		}, nil)
		producer.Flush(0)
		
		// Return API response
		c.JSON(http.StatusOK, gin.H{"message":"OK"})

	})

	r.GET("/coda/v0.01/ping", func(c *gin.Context) {
		// Return Json response
		c.JSON(200, gin.H{
			"message": "pong",
		})

		// Return string response
		//c.String(http.StatusOK, "hello")

	})


	r.Run(":8809") // listen and serve on 0.0.0.0:<PORT>
}

