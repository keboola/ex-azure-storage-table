<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Configuration;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getConnectionString(): string
    {
        return $this->getValue(['parameters', 'db', '#connectionString']);
    }
}
