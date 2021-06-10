/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"flag"
	"fmt"
	goparquet "github.com/fraugster/parquet-go"
	client "github.com/kelindar/talaria/client/golang"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
)

// ingestCmd represents the ingest command
var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Ingest a file using its URL",
	Run: func(cmd *cobra.Command, args []string) {
		inputDetails, err := getFileData(cmd, args)

		if err != nil {
			exitGracefully(err)
		}
		// Validating the file entered
		if _, err := checkIfValidFile(inputDetails.fileLocation); err != nil {
			exitGracefully(err)
		}

		fmt.Println("ingest called with arguments", inputDetails)

		c, err := New(inputDetails.talariaURL, &inputDetails.timeOut, &inputDetails.maxConcurrency, &inputDetails.errorPercentage)

		if err != nil {
			exitGracefully(err)
		}

		if inputDetails.useManualParquet {
			err = ingestParquetInManualOperation(c, inputDetails.fileLocation)
		} else {
			err = c.IngestURL(context.Background(), inputDetails.fileLocation)
		}

		if err != nil {
			exitGracefully(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(ingestCmd)

	ingestCmd.Flags().String("talariaURL", "www.talaria.net:8043", "Talaria URL")
	ingestCmd.Flags().Duration("timeout", 5, "Talaria Client Timeout")
	ingestCmd.Flags().Int("maxConcurrency", 10, "Talaria Client Concurrency")
	ingestCmd.Flags().Int("errorPercentage", 10, "Talaria Client Error Percentage")
	ingestCmd.Flags().Bool("useManualParquet", false, "Use Manual Parquet Ingestion")
}

func getFileData(cmd *cobra.Command, args []string) (inputDetails, error) {
	talariaURL, _ := cmd.Flags().GetString("talariaURL")
	timeOut, _ := cmd.Flags().GetDuration("timeout")
	maxConcurrency, _ := cmd.Flags().GetInt("maxConcurrency")
	errorPercentage, _ := cmd.Flags().GetInt("errorPercentage")
	useManualParquet, _ := cmd.Flags().GetBool("useManualParquet")

	flag.Parse() // This will parse all the arguments from the terminal

	fileLocation := args[0]

	// If we get to this endpoint, our program arguments are validated
	// We return the corresponding struct instance with all the required data
	return inputDetails{talariaURL, timeOut, maxConcurrency,
		errorPercentage, useManualParquet, fileLocation}, nil
}

func exitGracefully(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}

func checkIfValidFile(filename string) (bool, error) {
	// Checking if entered file is CSV by using the filepath package from the standard library
	if fileExtension := filepath.Ext(filename);
		fileExtension != ".orc" && fileExtension != ".parquet" && fileExtension != ".csv" {
		return false, fmt.Errorf("File %s is not valid", filename)
	}

	// If we get to this point, it means this is a valid file
	return true, nil
}

func ingestParquetInManualOperation(c *client.Client, filename string) (error) {
	if fileExtension := filepath.Ext(filename);
		fileExtension != ".parquet" {
		return fmt.Errorf("Manual Parquet Ingestion Operation Requested But File Is Not Parquet")
	}

	rf, err := os.Open(filename)
	defer rf.Close()

	fmt.Println("OPENING PARQUET")
	r, err := goparquet.NewFileReader(rf)

	if err != nil {
		fmt.Println("Error in opening is", err)
	}

	fmt.Println("Row count ", r.NumRows())

	for i := 0; i < int(r.NumRows()); i++ {
		data, err := r.NextRow()

		if err != nil {
			fmt.Println("Error is ", err)
		}

		//fmt.Println("Data is ", data)
		dataArray := make([]client.Event, 1)

		dataArray[0] = data

		fmt.Println("Ingesting row ", i)

		err = c.IngestBatch(context.Background(), dataArray)

		if err != nil {
			fmt.Println("Ingesting error is ", err)
		} else {
			fmt.Println("No error in ", i)
		}
	}

	return nil
}
