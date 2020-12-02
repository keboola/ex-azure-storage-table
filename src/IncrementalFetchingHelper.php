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
    public const STATE_INCREMENTAL_KEY = 'maxIncrementalKey';
    public const STATE_INCREMENTAL_VALUE = 'maxIncrementalValue';
    public const STATE_INCREMENTAL_VALUE_TYPE = 'maxIncrementalValueType';
    public const ALLOWED_TYPES = [
        EdmType::STRING,
        EdmType::INT32,
        EdmType::INT64,
        EdmType::DATETIME,
        EdmType::DOUBLE,
        EdmType::GUID,
    ];

    private Config $config;

    private LoggerInterface $logger;

    private string $dataDir;

    private array $inputState;

    private bool $enabled;

    /** @var mixed|null */
    protected $maxValue = null;

    /** @var int|null */
    protected $maxValueLength = null;

    /** @var string|null */
    protected $valueType = null;

    protected ?string $key;

    protected bool $valueTypeChecked = false;

    /**
     * @param mixed $value1
     * @param mixed $value2
     * @return mixed
     */
    public static function max($value1, $value2)
    {
        if (is_numeric($value1) && is_numeric($value2)) {
            return $value1 > $value2 ? $value1 : $value2;
        }

        return strnatcmp((string) $value1, (string) $value2) > 0 ?
            $value1 : $value2;
    }

    public function __construct(Config $config, LoggerInterface $logger, string $dataDir, array $inputState)
    {
        $this->config = $config;
        $this->logger = $logger;
        $this->dataDir = $dataDir;
        $this->inputState = $inputState;
        $this->enabled = $config->hasIncrementalFetchingKey();
        $this->key = $config->hasIncrementalFetchingKey() ? $config->getIncrementalFetchingKey() : null;
        $stateKey = $inputState[self::STATE_INCREMENTAL_KEY] ?? null;
        if ($stateKey && $stateKey !== $this->key) {
            $this->maxValue = null;
            $this->valueType = null;
        } else {
            $this->valueType = $inputState[self::STATE_INCREMENTAL_VALUE_TYPE] ?? null;
            $this->maxValue = $inputState[self::STATE_INCREMENTAL_VALUE] ?? null;
            $this->maxValueLength = $this->valueType === EdmType::STRING ? strlen($this->maxValue) : null;
        }
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

    /**
     * @return mixed
     */
    public function getValue()
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

        [$newValue, $newType] = $this->getIncValueAndType($item);

        // Null values are not allowed
        if ($newValue === null) {
            throw new UserException(sprintf(
                'Missing incremental fetching key "%s" in the row "%s".',
                $this->key,
                $rowIndex + 1
            ));
        }

        // Value type must be same for all rows
        if ($this->valueType && $newType !== $this->valueType) {
            throw new UserException(sprintf(
                'Incremental column type mismatch: "%s" and "%s" types found.',
                $this->valueType,
                $newType ?? '(null)'
            ));
        }

        // Check if is type allowed, for the first row
        if (!$this->valueTypeChecked) {
            if (!in_array($newType, self::ALLOWED_TYPES, true)) {
                throw new UserException(sprintf(
                    'Unexpected type "%s" of the incremental fetching "%s" key. Allowed types "%s".',
                    $newType,
                    $this->key,
                    implode('", "', self::ALLOWED_TYPES)
                ));
            }

            if ($newType === EdmType::STRING) {
                $this->logger->warning(sprintf(
                    'Warning: Key "%s" - type "%s" is used for incremental fetching. ' .
                    'For string type, all values must be the same length, ' .
                    'otherwise incremental fetching fails.',
                    $this->key,
                    $newType
                ));
            }

            $this->valueTypeChecked = true;
        }

        // Check if the string values have same length
        // it's because of how string comparison works.
        // Let's have string values "1" ... "1000"
        // - if we used a condition >= "80", database returns "9", "80"
        // - if we used a condition >= "99", database returns "99", but "100" is missing
        if ($newType === EdmType::STRING) {
            $newValueLength = strlen($newValue);
            if ($this->maxValue && $newValueLength !== $this->maxValueLength) {
                throw new UserException(sprintf(
                    'Unexpected value: Key "%s" - type "%s" is used for incremental fetching. ' .
                    'For string type, all values must be the same length. ' .
                    'This condition is not met, found "%s" (length=%d) and "%s" (length=%d). ' .
                    'Please use the same string lengths or a key with a different type: "%s".',
                    $this->key,
                    $newType,
                    $this->maxValue,
                    $this->maxValueLength,
                    $newValue,
                    $newValueLength,
                    implode('", "', array_diff(self::ALLOWED_TYPES, [EdmType::STRING]))
                ));
            }
            $this->maxValueLength = $newValueLength;
        }

        $this->maxValue = self::max($this->maxValue, $newValue);
        $this->valueType = $newType;
    }

    public function writeState(): void
    {
        if ($this->enabled && $this->maxValue) {
            JsonHelper::writeFile(
                $this->dataDir . '/out/state.json',
                [
                    self::STATE_INCREMENTAL_KEY => $this->key,
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

    private function getIncValueAndType(object $item): array
    {
        // Type is defined by annotation, or default types [Int32, Double, String] should be used.
        $value = $item->{$this->key} ?? null;
        $typeKey = $this->key . '@odata.type';
        switch (true) {
            case property_exists($item, $typeKey):
                $type = $item->{$typeKey};
                break;
            case is_int($value):
                $type = EdmType::INT32;
                break;
            case is_float($value):
                $type = EdmType::DOUBLE;
                break;
            case is_bool($value):
                $type = EdmType::BOOLEAN;
                break;
            default:
                $type = EdmType::STRING;
        }

        return [&$value, &$type];
    }
}
