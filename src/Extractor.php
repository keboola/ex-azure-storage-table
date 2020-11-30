<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\Exception\UserException;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Table\Internal\ITable;

class Extractor
{
    private Config $config;

    private ITable $tableClient;

    public function __construct(Config $config, TableClientFactory $clientFactory)
    {
        $this->config = $config;
        $this->tableClient = $clientFactory->create();
    }

    public function testConnection(): void
    {
        try {
            $this->tableClient->queryTables();
        } catch (ServiceException $e) {
            throw UserException::from($e, $this->config->getConnectionString());
        }
    }

    public function extract(): void
    {
    }
}
