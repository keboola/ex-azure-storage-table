# Azure Storage Table Extractor

[![Build Status](https://travis-ci.com/keboola/ex-azure-storage-table.svg?branch=master)](https://travis-ci.com/keboola/ex-azure-storage-table)

Extracts data  from
- [Azure Cosmos DB Table API](https://docs.microsoft.com/en-us/azure/cosmos-db/table-introduction)
- [Azure Table storage](https://docs.microsoft.com/en-us/azure/cosmos-db/table-support)

... to the [Keboola Connection](https://www.keboola.com).

## Configuration

The configuration `config.json` contains following properties in `parameters` key: 
- `db` - object (required): Configuration of the connection.
    - `#connectionString` - string (required): Connection string to Azure Table storage or Azure Cosmos DB Table API.
- `table` - string (required): Name of the input table in the API.
- `output` - string (required): Name of the output CSV file.
- `maxTries`- integer (optional): Number of the max retries if an error occurred. Default `5`.
- `incremental` - boolean (optional): Enables [Incremental Loading](https://help.keboola.com/storage/tables/#incremental-loading). Default `false`.
- `incrementalFetchingKey` - string (optional): Name of the key for [Incremental Fetching](https://help.keboola.com/components/extractors/database/#incremental-fetching).
- `mode` - enum (optional)
    - `mapping` (default) - Rows are exported using specified `mapping`, [read more](https://github.com/keboola/php-csvmap).
    - `raw` - Rows are exported as plain JSON strings. CSV file will contain `PartitionKey`, `RowKey` and `data` columns.
- `mapping` - string - required for `mode` = `mapping`, [read more](https://github.com/keboola/php-csvmap).

- By default, extractor exports all rows and columns. It can be adjusted using these settings.
    - `select` - string (optional), eg. `PartitionKey, RowKey, Name, Age`.
       - For `raw` mode must be `PartitionKey` and `RowKey` fields present in the query results.
    - `limit` - integer (optional), maximum number of the exported rows, eg. `500`.   
    - `filter` - string (optional), [OData query $filter](https://docs.microsoft.com/en-us/azure/search/search-query-odata-filter), eg. `RowKey ge '2' and age gt 17`


## Actions

Read more about actions [in KBC documentation](https://developers.keboola.com/extend/common-interface/actions/).

### Test Connection

Action `testConnection` tests the connection to the server.

The `parameters.db` node must be specified in the configuration.

## Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/ex-azure-storage-table
cd ex-azure-storage-table
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```


Create `.env` file with following variables:
```env
CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=..."
```


Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
 
# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 

## License

MIT licensed, see [LICENSE](./LICENSE) file.
