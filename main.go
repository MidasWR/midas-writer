package main

import (
	"edge-beta/log"
	"fmt"
	"github.com/rs/zerolog"
	"io/ioutil"
	"math/rand"
	"time"
)

func main() {
	t := time.Now()
	wrt, err := log.InitWriter("localhost:50051")
	if err != nil {
		panic(err)
	}
	lg := zerolog.New(&wrt)
	defer func() {
		fmt.Println("Time elapsed:", time.Since(t))
	}()

	lg.Info().Msg("Starting Order Processing Service")
	lg.Info().Msg("Loading configuration from /etc/app/config.yaml")

	_, err = ioutil.ReadFile("/etc/app/config.yaml")
	if err != nil {
		lg.Error().Err(err).Msg("Failed to read configuration file")
	}

	lg.Info().Msg("Connecting to PostgreSQL database...")
	time.Sleep(100 * time.Millisecond)
	lg.Info().Msg("Database connection established")

	lg.Info().Msg("Connecting to Redis cache...")
	time.Sleep(50 * time.Millisecond)
	lg.Info().Msg("Redis cache ready")

	lg.Info().Msg("Starting background job scheduler")
	lg.Info().Msg("Loading previous failed jobs from storage")
	lg.Info().Msg("Recovered 3 pending retry jobs")

	lg.Info().Msg("Listening for incoming order requests on port 8080")

	// Симулируем 10 заказов
	for i := 1; i <= 10; i++ {
		orderID := fmt.Sprintf("ORD-%05d", i)
		lg.Info().Str("order_id", orderID).Msg("Received new order request")

		lg.Info().Str("order_id", orderID).Msg("Validating order input")
		if rand.Intn(10) > 7 {
			lg.Warn().Str("order_id", orderID).Msg("Order missing optional field 'notes'")
		}
		lg.Info().Str("order_id", orderID).Msg("Validation complete")

		lg.Info().Str("order_id", orderID).Msg("Checking customer account balance")
		if rand.Intn(10) > 8 {
			lg.Error().Str("order_id", orderID).Msg("Insufficient balance detected")
			lg.Info().Str("order_id", orderID).Msg("Sending notification about payment issue")
			continue
		}

		lg.Info().Str("order_id", orderID).Msg("Reserving stock in inventory service")
		if rand.Intn(10) > 8 {
			lg.Error().Str("order_id", orderID).Msg("Inventory reservation failed")
			lg.Info().Str("order_id", orderID).Msg("Scheduling retry for stock reservation")
			continue
		}

		lg.Info().Str("order_id", orderID).Msg("Stock reserved successfully")
		lg.Info().Str("order_id", orderID).Msg("Calculating shipping cost")
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)

		lg.Info().Str("order_id", orderID).Msg("Shipping cost calculated successfully")

		if rand.Intn(10) > 7 {
			lg.Warn().Str("order_id", orderID).Msg("Customer address incomplete, requesting update")
			lg.Info().Str("order_id", orderID).Msg("Waiting for user address confirmation")
		}

		lg.Info().Str("order_id", orderID).Msg("Initiating payment transaction")
		if rand.Intn(10) > 8 {
			lg.Error().Str("order_id", orderID).Msg("Payment gateway timeout")
			lg.Info().Str("order_id", orderID).Msg("Retrying payment in 3 seconds")
			time.Sleep(3 * time.Second)
			lg.Info().Str("order_id", orderID).Msg("Retry successful")
		}
		lg.Info().Str("order_id", orderID).Msg("Payment confirmed")

		lg.Info().Str("order_id", orderID).Msg("Generating shipping label")
		time.Sleep(20 * time.Millisecond)
		lg.Info().Str("order_id", orderID).Msg("Shipping label created")

		if rand.Intn(10) > 6 {
			lg.Info().Str("order_id", orderID).Msg("Sending email confirmation to customer")
			lg.Info().Str("order_id", orderID).Msg("Email sent successfully")
		}

		lg.Info().Str("order_id", orderID).Msg("Updating order status in database")
		lg.Info().Str("order_id", orderID).Msg("Order marked as 'Processed'")

		lg.Info().Str("order_id", orderID).Msg("Publishing order event to Kafka topic 'orders.completed'")
		lg.Info().Str("order_id", orderID).Msg("Event successfully published")

		if rand.Intn(10) > 8 {
			lg.Warn().Str("order_id", orderID).Msg("Notification service responded slowly")
		}

		lg.Info().Str("order_id", orderID).Msg("Order workflow completed successfully")
	}

	lg.Info().Msg("All orders processed")
	lg.Info().Msg("Flushing metrics and closing connections")
	lg.Info().Msg("Gracefully shutting down service")

	_, err = ioutil.ReadFile("/nonexistent/file")
	lg.Error().Err(err).Msg("Failed to read metrics snapshot file")

	lg.Info().Msg("Service shutdown complete")
}
