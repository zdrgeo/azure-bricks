package main

import (
	"context"
	"errors"
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
	notifyContext, cancelNotify := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelNotify()

	ingestion, err := azkustoingest.New(connectionStringBuilder, azkustoingest.WithDefaultDatabase("database"), azkustoingest.WithDefaultTable("table"))
	if err != nil {
		log.Panic(err)
	}

	defer ingestion.Close()

	ticker := time.NewTicker(time.Minute)

	defer ticker.Stop()

	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			select {
			case <-notifyContext.Done():
				break
			case <-ticker.C:
				fileName := ""

				if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
					continue
				}

				result, err := ingestion.FromFile(notifyContext, fileName, azkustoingest.DeleteSource())

				if err != nil {
					log.Panic(err)
				}

				err = <-result.Wait(notifyContext)

				if err != nil {
					log.Panic(err)
				}
			}
		}
	}()

	<-done
}
