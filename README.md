# TalariaFileIngestionClient
File Ingestion Client for Talaria

## Installation

<code> git clone https://github.com/atris/TalariaFileIngestionClient  </code>
<br>
<code> cd TalariaFileIngestionClient </code>
<br>
<code> go install TalariaFileIngestionClient </code>

## Usage

<code> TalariaFileIngestionClient ingest {flags} {file URLs} </code>

NOTE: For each file URL, TalariaFileIngestionClient will fire a separate thread on the client machine. Please ensure that you specify a file count which does not cause your CPU cores to be exceeded.
  
Flags:

      --errorPercentage int   Talaria Client Error Percentage (default 10)
      --maxConcurrency int    Talaria Client Concurrency (default 10)
      --talariaURL string     Talaria URL (default "www.talaria.net:8043")
      --timeout duration      Talaria Client Timeout (default 5ns)
      --useManualParquet      Use Manual Parquet Ingestion
  
  
## Help
  
<code> TalariaIngestionClient help ingest <code>
  
Use the Manual Parquet Ingestion if you wish to see details of what is getting ingested. Recommended for debugging
