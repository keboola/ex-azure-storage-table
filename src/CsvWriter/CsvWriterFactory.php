<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\CsvWriter;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\Configuration\ConfigDefinition;
use Keboola\AzureStorageTableExtractor\Exception\UnexpectedValueException;
use Keboola\AzureStorageTableExtractor\IncrementalFetchingHelper;
use Keboola\Component\Manifest\ManifestManager;

class CsvWriterFactory
{
    private string $dataDir;

    private ManifestManager $manifestManager;

    private Config $config;

    private IncrementalFetchingHelper $incFetchingHelper;

    public function __construct(
        string $dataDir,
        ManifestManager $manifestManager,
        Config $config,
        IncrementalFetchingHelper $incFetchingHelper
    ) {
        $this->dataDir = $dataDir;
        $this->manifestManager = $manifestManager;
        $this->config = $config;
        $this->incFetchingHelper = $incFetchingHelper;
    }

    public function create(): ICsvWriter
    {
        switch ($this->config->getMode()) {
            case ConfigDefinition::MODE_RAW:
                return new RawCsvWriter(
                    $this->dataDir,
                    $this->manifestManager,
                    $this->config,
                    $this->incFetchingHelper
                );
            case ConfigDefinition::MODE_MAPPING:
                return new MappingCsvWriter(
                    $this->dataDir,
                    $this->manifestManager,
                    $this->config,
                    $this->incFetchingHelper
                );
        }

        throw new UnexpectedValueException(sprintf('Unexpected mode "%s".', $this->config->getMode()));
    }
}
