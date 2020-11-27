<?php

namespace Keboola\AzureStorageTableExtractor;


use MicrosoftAzure\Storage\Table\TableRestProxy;

class Extractor
{
    public function extract(): void
    {
        $connectionString = '';
        $tableClient = TableRestProxy::createTableService($connectionString);
        $tableClient->queryEntitiesAsync();
    }
}
