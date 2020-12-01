<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Configuration;

use Keboola\AzureStorageTableExtractor\Exception\UndefinedValueException;
use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public const STATE_INCREMENTAL_VALUE = 'maxIncrementalValue';

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

    public function hasQuery(): bool
    {
        return $this->getValue(['parameters', 'query']) !== null;
    }

    public function getQuery(): string
    {
        if (!$this->hasQuery()) {
            throw new UndefinedValueException('Query is not defined.');
        }

        return $this->getValue(['parameters', 'query']);
    }

    public function getMode(): string
    {
        return $this->getValue(['parameters', 'mode']);
    }

    public function getMapping(): array
    {
        if ($this->getMode() !== ConfigDefinition::MODE_MAPPING) {
            throw new UndefinedValueException('Mode is not set to mapping.');
        }

        return $this->getValue(['parameters', 'mapping']);
    }

    public function isIncremental(): bool
    {
        return $this->getValue(['parameters', 'incremental']);
    }

    public function hasIncrementalFetchingKey(): bool
    {
        return $this->getValue(['parameters', 'incrementalFetchingKey']) !== null;
    }

    public function getIncrementalFetchingKey(): string
    {
        if (!$this->hasIncrementalFetchingKey()) {
            throw new UndefinedValueException('IncrementalFetchingKey is not defined.');
        }

        return $this->getValue(['parameters', 'incrementalFetchingKey']);
    }

    public function hasSelect(): bool
    {
        return $this->getValue(['parameters', 'select']) !== null;
    }

    public function getSelect(): string
    {
        if (!$this->hasSelect()) {
            throw new UndefinedValueException('Select is not defined.');
        }

        return $this->getValue(['parameters', 'select']);
    }

    public function hasLimit(): bool
    {
        return $this->getValue(['parameters', 'limit']) !== null;
    }

    public function getLimit(): int
    {
        if (!$this->hasLimit()) {
            throw new UndefinedValueException('Limit is not defined.');
        }

        return (int) $this->getValue(['parameters', 'limit']);
    }
}
