<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\CsvWriter;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\IncrementalFetchingHelper;
use Keboola\Component\Manifest\ManifestManager;

abstract class BaseCsvWriter implements ICsvWriter
{
    protected string $dataDir;

    protected ManifestManager $manifestManager;

    protected Config $config;

    protected IncrementalFetchingHelper $incFetchingHelper;

    protected int $rowCount = 0;

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
        $this->init();
    }

    abstract protected function init(): void;

    public function writeItem(object $item): void
    {
        $this->incFetchingHelper->processItem($item, $this->rowCount);
        $this->rowCount++;
    }

    protected function unsetODataMetadata(object $item): void
    {
        foreach ($item as $key => $value) {
            if (strpos($key, 'odata.') !== false) {
                unset($item->{$key});
            }
        }
    }
}
