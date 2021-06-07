package cmd

import (
	talaria "github.com/kelindar/talaria/client/golang"
	"github.com/myteksi/hystrix-go/hystrix"
	"time"
)

func New(endpoint string, circuitTimeout *time.Duration, maxConcurrent *int, errorPercentThreshold *int) (*talaria.Client, error) {

	var newTimeout = 5 * time.Second
	var newMaxConcurrent = hystrix.DefaultMaxConcurrent
	var newErrorPercentThreshold = hystrix.DefaultErrorPercentThreshold

	// Set defaults for variables if there aren't any
	if circuitTimeout != nil {
		newTimeout = *circuitTimeout * time.Second
	}

	if maxConcurrent != nil {
		newMaxConcurrent = *maxConcurrent
	}

	if errorPercentThreshold != nil {
		newErrorPercentThreshold = *errorPercentThreshold
	}

	dialOptions := []talaria.Option{}
	dialOptions = append(dialOptions, talaria.WithCircuit(newTimeout, newMaxConcurrent, newErrorPercentThreshold))

	client, err := getClient(endpoint, dialOptions...)

	if err != nil {
		return nil, err
	}

	return client, nil
}

func getClient(endpoint string, options ...talaria.Option) (*talaria.Client, error) {
	client, err := talaria.Dial(endpoint, options...)

	if err != nil {
		return nil, err
	}
	return client, nil
}
