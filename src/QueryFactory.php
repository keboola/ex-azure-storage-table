<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use MicrosoftAzure\Storage\Table\Models\Query;

class QueryFactory
{
    private Config $config;

    public function __construct(Config $config)
    {
        $this->config = $config;
    }

    public function create(): Query
    {
        $query = new Query();
        return $query;
    }
}
