<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\CsvWriter;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\IncrementalFetchingHelper;

abstract class BaseCsvWriter implements ICsvWriter
{
    protected string $dataDir;

    protected Config $config;

    protected IncrementalFetchingHelper $incFetchingHelper;

    protected int $rowCount = 0;

    public function __construct(string $dataDir, Config $config, IncrementalFetchingHelper $incFetchingHelper)
    {
        $this->dataDir = $dataDir;
        $this->config = $config;
        $this->incFetchingHelper = $incFetchingHelper;
    }

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
