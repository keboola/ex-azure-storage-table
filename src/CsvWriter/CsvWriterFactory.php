<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\CsvWriter;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\Configuration\ConfigDefinition;
use Keboola\AzureStorageTableExtractor\Exception\UnexpectedValueException;

class CsvWriterFactory
{
    private string $dataDir;

    private Config $config;

    public function __construct(string $dataDir, Config $config)
    {
        $this->dataDir = $dataDir;
        $this->config = $config;
    }

    public function create(): ICsvWriter
    {
        switch ($this->config->getMode()) {
            case ConfigDefinition::MODE_RAW:
                return new RawCsvWriter($this->dataDir, $this->config);
            case ConfigDefinition::MODE_MAPPING:
                return new MappingCsvWriter($this->dataDir, $this->config);
        }

        throw new UnexpectedValueException(sprintf('Unexpected mode "%s".', $this->config->getMode()));
    }
}
