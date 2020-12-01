<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\CsvWriter;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\Exception\ApplicationException;
use Keboola\AzureStorageTableExtractor\Exception\UserException;
use Keboola\AzureStorageTableExtractor\IncrementalFetchingHelper;
use Keboola\Component\JsonHelper;
use Keboola\Csv\CsvWriter;

class RawCsvWriter extends BaseCsvWriter implements ICsvWriter
{
    public const ITEM_PARTITION_KEY_COLUMN = 'PartitionKey';
    public const ITEM_ROW_KEY_COLUMN = 'RowKey';
    public const PARTITION_KEY_COLUMN = 'PartitionKey';
    public const ROW_KEY_COLUMN = 'RowKey';
    public const DATA_COLUMN = 'data';

    private string $csvPath;

    private CsvWriter $writer;

    public function __construct(string $dataDir, Config $config, IncrementalFetchingHelper $incFetchingHelper)
    {
        parent::__construct($dataDir, $config, $incFetchingHelper);
        $this->csvPath = sprintf('%s/out/tables/%s.csv', $dataDir, $config->getOutput());
        $this->writer = new CsvWriter($this->csvPath);
    }

    public function __destruct()
    {
        // No rows -> no CSV file
        if ($this->rowCount === 0) {
            @unlink($this->csvPath);
        }
    }

    public function writeItem(object $item): void
    {
        parent::writeItem($item);
        $this->unsetODataMetadata($item);

        [$partitionKey, $rowKey] = $this->getPrimaryKey($item);

        // Write row to CSV
        $this->writer->writeRow([
            self::PARTITION_KEY_COLUMN => $partitionKey,
            self::ROW_KEY_COLUMN => $rowKey,
            self::DATA_COLUMN => json_encode($item),
        ]);
    }

    public function finalize(): void
    {
        $this->writeManifest();
    }

    public function writeManifest(): void
    {
        if ($this->rowCount > 0) {
            $manifestPath = $this->csvPath . '.manifest';
            file_put_contents($manifestPath, JsonHelper::encode($this->getManifest(), true));
        }
    }

    protected function getManifest(): array
    {
        return [
            'columns' => [self::PARTITION_KEY_COLUMN, self::ROW_KEY_COLUMN, self::DATA_COLUMN],
            'primary_key' => [self::PARTITION_KEY_COLUMN, self::ROW_KEY_COLUMN],
            'incremental' => $this->config->isIncremental(),
        ];
    }

    protected function getPrimaryKey(object $item): array
    {
        foreach ([self::ITEM_PARTITION_KEY_COLUMN, self::ITEM_ROW_KEY_COLUMN] as $key) {
            if (!property_exists($item, $key)) {
                if ($this->config->hasSelect()) {
                    // ID is missing, because it is not configured in the "select"
                    throw new UserException(sprintf(
                        'Missing "%s" key in the query results. ' .
                        'Please modify the "select" value in the configuration ' .
                        'or use the "mapping" mode instead of the "raw".',
                        $key
                    ));
                } else {
                    throw new ApplicationException(sprintf('Missing "%s" key in the query results.', $key));
                }
            }
        }

        $partitionKey = $item->{self::ITEM_PARTITION_KEY_COLUMN};
        $rowKey = $item->{self::ITEM_ROW_KEY_COLUMN};
        return [$partitionKey, $rowKey];
    }
}
