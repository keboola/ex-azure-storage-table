<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\CsvWriter;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\Configuration\ConfigDefinition;
use Keboola\AzureStorageTableExtractor\Exception\UnexpectedValueException;
use Keboola\AzureStorageTableExtractor\IncrementalFetchingHelper;

class CsvWriterFactory
{
    private string $dataDir;

    private Config $config;

    private IncrementalFetchingHelper $incFetchingHelper;

    public function __construct(string $dataDir, Config $config, IncrementalFetchingHelper $incFetchingHelper)
    {
        $this->dataDir = $dataDir;
        $this->config = $config;
        $this->incFetchingHelper = $incFetchingHelper;
    }

    public function create(): ICsvWriter
    {
        switch ($this->config->getMode()) {
            case ConfigDefinition::MODE_RAW:
                return new RawCsvWriter($this->dataDir, $this->config, $this->incFetchingHelper);
            case ConfigDefinition::MODE_MAPPING:
                return new MappingCsvWriter($this->dataDir, $this->config, $this->incFetchingHelper);
        }

        throw new UnexpectedValueException(sprintf('Unexpected mode "%s".', $this->config->getMode()));
    }
}
