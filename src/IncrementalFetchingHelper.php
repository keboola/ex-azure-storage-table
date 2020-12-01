<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\Exception\UndefinedValueException;
use Keboola\AzureStorageTableExtractor\Exception\UserException;
use Keboola\Component\JsonHelper;
use MicrosoftAzure\Storage\Table\Models\EdmType;
use Psr\Log\LoggerInterface;

class IncrementalFetchingHelper
{
    public const STATE_INCREMENTAL_VALUE = 'maxIncrementalValue';
    public const STATE_INCREMENTAL_VALUE_TYPE = 'maxIncrementalValueType';

    private Config $config;

    private LoggerInterface $logger;

    private string $dataDir;

    private array $inputState;

    private bool $enabled;

    /** @var mixed|null */
    protected $maxValue = null;

    /** @var string|null */
    protected $valueType = null;

    protected ?string $key;


    public function __construct(Config $config, LoggerInterface $logger, string $dataDir, array $inputState)
    {
        $this->config = $config;
        $this->logger = $logger;
        $this->dataDir = $dataDir;
        $this->inputState = $inputState;
        $this->enabled = $config->hasIncrementalFetchingKey();
        $this->key = $config->hasIncrementalFetchingKey() ? $config->getIncrementalFetchingKey() : null;
        $this->maxValue = $inputState[self::STATE_INCREMENTAL_VALUE] ?? null;
        $this->valueType = $inputState[self::STATE_INCREMENTAL_VALUE_TYPE] ?? null;
    }

    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    public function hasValue(): bool
    {
        return $this->enabled && $this->maxValue;
    }

    public function getKey(): string
    {
        if (!$this->key) {
            throw new UndefinedValueException('Incremental fetching key is not defined.');
        }

        return $this->key;
    }

    public function getValue(): string
    {
        if (!$this->hasValue()) {
            throw new UndefinedValueException('Incremental fetching max value is not defined.');
        }

        return $this->maxValue;
    }

    public function getValueType(): string
    {
        if (!$this->valueType) {
            throw new UndefinedValueException('Incremental fetching value type is not defined.');
        }

        return $this->valueType;
    }

    public function processItem(object $item, int $rowIndex): void
    {
        if (!$this->enabled) {
            return;
        }

        $newValue = $item->{$this->key} ?? null;
        $newType = $item->{$this->key . '@odata.type'}  ?? EdmType::STRING;

        if ($newValue === null) {
            throw new UserException(sprintf(
                'Missing incremental fetching key "%s" in the row "%s".',
                $this->key,
                $rowIndex + 1
            ));
        }

        if ($this->valueType && $newType !== $this->valueType) {
            throw new UserException(sprintf(
                'Incremental column type mismatch: "%s" and "%s" types found.',
                $this->valueType,
                $newType ?? '(null)'
            ));
        }

        $this->maxValue = max($this->maxValue, $newValue);
        $this->valueType = $newType;
    }

    public function writeState(): void
    {
        if ($this->enabled && $this->maxValue) {
            JsonHelper::writeFile(
                $this->dataDir . '/out/state.json',
                [
                    self::STATE_INCREMENTAL_VALUE => $this->maxValue,
                    self::STATE_INCREMENTAL_VALUE_TYPE => $this->valueType,
                ]
            );
            $this->logger->info(sprintf(
                'Incremental fetching: new state "%s" = "%s" (%s)',
                $this->key,
                $this->maxValue,
                $this->valueType
            ));
        }
    }
}
