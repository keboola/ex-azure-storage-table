<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Configuration;

use InvalidArgumentException;
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

    public function hasFilter(): bool
    {
        return $this->getValue(['parameters', 'filter']) !== null;
    }

    public function getFilter(): string
    {
        if (!$this->hasFilter()) {
            throw new UndefinedValueException('Filter is not defined.');
        }

        return $this->getValue(['parameters', 'filter']);
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
        try {
            return $this->getValue(['parameters', 'incrementalFetchingKey']) !== null;
        } catch (InvalidArgumentException $e) {
            return false;
        }
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

    public function getSelect(): array
    {
        if (!$this->hasSelect()) {
            throw new UndefinedValueException('Select is not defined.');
        }

        return array_map(
            fn(string $field) => trim($field),
            explode(',', $this->getValue(['parameters', 'select']))
        );
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
