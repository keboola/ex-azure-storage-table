<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Configuration;

use Keboola\AzureStorageTableExtractor\Exception\UndefinedValueException;
use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getConnectionString(): string
    {
        return $this->getValue(['parameters', 'db', '#connectionString']);
    }

    public function getTable(): string
    {
        return $this->getValue(['parameters', 'table']);
    }

    public function getOutput(): string
    {
        return $this->getValue(['parameters', 'output']);
    }

    public function getMaxTries(): int
    {
        return (int) $this->getValue(['parameters', 'maxTries']);
    }

    public function isIncremental(): bool
    {
        return $this->getValue(['parameters', 'incremental']);
    }

    public function hasIncrementalFetchingColumn(): bool
    {
        return $this->getValue(['parameters', 'incrementalFetchingColumn']) !== null;
    }

    public function getIncrementalFetchingColumn(): string
    {
        if (!$this->hasIncrementalFetchingColumn()) {
            throw new UndefinedValueException('IncrementalFetchingColumn is not defined.');
        }

        return $this->getValue(['parameters', 'incrementalFetchingColumn']);
    }
}
