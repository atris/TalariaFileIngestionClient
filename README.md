# TalariaFileIngestionClient
File Ingestion Client for Talaria

## Installation

<code> go get -u github.com/atris/TalariaFileIngestionClient </code>

## Usage

<code> TalariaIngestionClient ingest {flags} {file URL} </code>
  
Flags:

      --errorPercentage int   Talaria Client Error Percentage (default 10)
      --maxConcurrency int    Talaria Client Concurrency (default 10)
      --talariaURL string     Talaria URL (default "www.talaria.net:8043")
      --timeout duration      Talaria Client Timeout (default 5ns)
  
  
## Help
  
<code> TalariaIngestionClient help ingest <code>
