package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustoingest"
	"github.com/spf13/viper"
)

var (
	connectionStringBuilder *azkustodata.ConnectionStringBuilder
)

func init() {
	viper.AddConfigPath(".")
	// viper.SetConfigFile(".env")
	// viper.SetConfigName("config")
	// viper.SetConfigType("env") // "env", "json", "yaml"
	viper.SetEnvPrefix("demo")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Panic(err)
	}

	// connectionStringBuilder = azkustodata.NewConnectionStringBuilder(viper.GetString("AZURE_DATAEXPLORER_CONNECTION_STRING")).WithDefaultAzureCredential()
	connectionStringBuilder = azkustodata.NewConnectionStringBuilder(viper.GetString("AZURE_DATAEXPLORER_CONNECTION_STRING"))
}

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelCtx()

	ingestion, err := azkustoingest.New(connectionStringBuilder, azkustoingest.WithDefaultDatabase("database"), azkustoingest.WithDefaultTable("table"))

	if err != nil {
		log.Panic(err)
	}

	defer ingestion.Close()

	tick := time.Tick(1 * time.Minute)

	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case <-tick:
			fileName := ""

			if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
				continue
			}

			result, err := ingestion.FromFile(ctx, fileName, azkustoingest.DeleteSource())

			if err != nil {
				log.Panic(err)
			}

			err = <-result.Wait(ctx)

			if err != nil {
				log.Panic(err)
			}
		}
	}
}

func streamingOrManagedMain() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelCtx()

	ingestion, err := azkustoingest.NewStreaming(connectionStringBuilder, azkustoingest.WithDefaultDatabase("database"), azkustoingest.WithDefaultTable("table"))
	// ingestion, err := azkustoingest.NewManaged(connectionStringBuilder, azkustoingest.WithDefaultDatabase("database"), azkustoingest.WithDefaultTable("table"))

	if err != nil {
		log.Panic(err)
	}

	defer ingestion.Close()

	tick := time.Tick(1 * time.Minute)

	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case <-tick:
			reader, writer := io.Pipe()

			encoder := json.NewEncoder(writer)

			go func() {
				defer writer.Close()

				records := createRecords()

				for _, record := range records {
					if err := encoder.Encode(record); err != nil {
						log.Panic(err)
					}
				}
			}()

			result, err := ingestion.FromReader(ctx, reader)

			if err != nil {
				log.Panic(err)
			}

			err = <-result.Wait(ctx)

			if err != nil {
				log.Panic(err)
			}
		}
	}
}

func createRecords() []struct{} {
	records := []struct{}{}

	return records
}
