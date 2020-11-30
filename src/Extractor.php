<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use Keboola\Component\UserException;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Table\Internal\ITable;

class Extractor
{
    private ITable $tableClient;

    public function __construct(TableClientFactory $clientFactory)
    {
        $this->tableClient = $clientFactory->create();
    }

    public function testConnection(): void
    {
        try {
            $this->tableClient->queryTables();
        } catch (ServiceException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
    }

    public function extract(): void
    {
    }
}
