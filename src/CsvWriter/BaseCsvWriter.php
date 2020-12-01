<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\CsvWriter;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\Exception\UserException;
use Keboola\Component\JsonHelper;

abstract class BaseCsvWriter implements ICsvWriter
{
    protected string $dataDir;

    protected Config $config;

    /** @var mixed|null */
    protected $maxIncrementalValue = null;

    protected ?string $incrementalFetchingKey;

    protected int $rowCount = 0;

    public function __construct(string $dataDir, Config $config)
    {
        $this->dataDir = $dataDir;
        $this->config = $config;
        $this->incrementalFetchingKey =
            $config->hasIncrementalFetchingKey() ? $config->getIncrementalFetchingKey() : null;
    }

    public function writeItem(object $item): void
    {
        if ($this->incrementalFetchingKey) {
            $incrementalValue = property_exists($item, $this->incrementalFetchingKey) ?
                $item->{$this->incrementalFetchingKey} : null;

            if ($incrementalValue === null) {
                throw new UserException(sprintf(
                    'Missing incremental fetching key "%s" in the row "%s".',
                    $this->incrementalFetchingKey,
                    $this->rowCount + 1
                ));
            }

            // Storage table API has no support for ORDER BY,
            // ... so we store max instead of the last value
            $this->maxIncrementalValue = max($this->maxIncrementalValue, $incrementalValue);
        }
        $this->rowCount++;
    }

    public function writeLastState(array $inputState): void
    {
        $incrementalValue = null;
        if ($this->maxIncrementalValue) {
            $incrementalValue = $this->maxIncrementalValue;
        } elseif (isset($inputState[Config::STATE_INCREMENTAL_VALUE])) {
            $incrementalValue = $inputState[Config::STATE_INCREMENTAL_VALUE];
        }

        if ($incrementalValue) {
            JsonHelper::writeFile(
                $this->dataDir . '/out/state.json',
                [
                    Config::STATE_INCREMENTAL_VALUE => $incrementalValue,
                ]
            );
        }
    }
}
