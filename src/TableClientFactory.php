<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use Keboola\Component\UserException;
use RuntimeException;
use Keboola\AzureStorageTableExtractor\Configuration\Config;
use MicrosoftAzure\Storage\Table\Internal\ITable;
use MicrosoftAzure\Storage\Table\TableRestProxy;

class TableClientFactory
{
    private Config $config;

    public function __construct(Config $config)
    {
        $this->config = $config;
    }

    public function create(): ITable
    {
        try {
            return TableRestProxy::createTableService($this->config->getConnectionString());
        } catch (RuntimeException $e) {
            throw new UserException('Connection error: ' . $e->getMessage(), $e->getCode(), $e);
        }
    }
}
