<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Tests\Data;

use Generator;
use DateTimeImmutable;
use SplFileInfo;
use RuntimeException;
use MicrosoftAzure\Storage\Table\Models\EdmType;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Table\Internal\ITable;
use MicrosoftAzure\Storage\Table\Models\Entity;
use MicrosoftAzure\Storage\Table\Models\BatchOperations;
use MicrosoftAzure\Storage\Table\TableRestProxy;
use Symfony\Component\Finder\Finder;
use Keboola\Csv\CsvReader;

class DataUploader
{
    private ITable $client;

    public function __construct()
    {
        $this->client = TableRestProxy::createTableService((string) getenv('CONNECTION_STRING'));
    }

    public function uploadFromDir(string $dir): void
    {
        $finder = new Finder();
        $csvFiles = $finder->files()->in($dir)->name('*.csv');
        foreach ($csvFiles as $csvFile) {
            $this->upload($csvFile);
        }
    }

    public function upload(SplFileInfo $csvFile): void
    {
        $tableName = (string) preg_replace('~\.csv$~', '', $csvFile->getFilename());
        $this->createTable($tableName);

        printf('Inserting rows from "%s" to table "%s" ... ', $csvFile->getFilename(), $tableName);
        $csvReader = new CsvReader($csvFile->getPathname());
        $csvIterator = new \NoRewindIterator($csvReader);
        $header = $csvReader->getHeader();
        $csvIterator->next(); // skip header

        // First row after header is column type
        $types = $csvIterator->current();
        $csvIterator->next();

        $batch = new BatchOperations();
        foreach ($csvIterator as $row) {
            $batch->addInsertOrReplaceEntity($tableName, $this->rowToEntity($row, $header, $types));
        }
        $this->client->batch($batch);
        echo "OK\n";
    }

    public function uploadBigTable(): void
    {
        $tableName = 'big-table';
        $this->createTable($tableName);

        printf('Inserting rows to "%s" ... ', $tableName);
        $entities = $this->generateEntities();
        $batchSize = 1000;
        $batchIndex = 1;
        while ($entities->valid()) {
            $batch = new BatchOperations();
            for ($i = 0; $i < $batchSize && $entities->valid(); $i++) {
                /** @var Entity $entity */
                $entity = $entities->current();
                $entity->setPartitionKey($entity->getPartitionKey() . '_' . $batchIndex);
                $batch->addInsertOrReplaceEntity($tableName, $entity);
                $entities->next();
            }
            $this->client->batch($batch);
            $batchIndex++;
            echo '+';
        }

        echo " OK\n";
    }

    private function createTable(string $tableName): void
    {
        printf('Creating table "%s" ... ', $tableName);
        try {
            $this->client->createTable($tableName);
            echo "OK\n";
        } catch (ServiceException $e) {
            if (strpos($e->getMessage(), 'already exists') !== false) {
                echo "EXISTS\n";
                return;
            }

            if (strpos($e->getMessage(), 'not supported for serverless accounts.') !== false) {
                throw new RuntimeException(sprintf(
                    'Database is running in the serverless mode. Cannot create table. ' .
                    'Please create table "%s" manually in the Azure Portal.',
                    $tableName
                ));
            }

            throw $e;
        }
    }

    private function rowToEntity(array &$row, array &$header, array &$types): Entity
    {
        $entity = new Entity();
        foreach ($row as $index => $cell) {
            $key = $header[$index];
            $type = $types[$index];

            // Ignore empty cells
            if ($cell === '') {
                continue;
            }

            // Convert datetime
            if ($type === EdmType::DATETIME) {
                $cell = DateTimeImmutable::createFromFormat('Y-m-d\TH:i:s', $cell);
            } elseif ($type === EdmType::BOOLEAN) {
                $cell = $cell === '1';
            }

            if ($key === 'PartitionKey') {
                $entity->setPartitionKey((string) $cell);
            } elseif ($key === 'RowKey') {
                $entity->setRowKey((string) $cell);
            } else {
                $entity->addProperty($key, $type, $cell);
            }
        }

        return $entity;
    }

    /**
     * @return Generator|Entity[]
     */
    public function generateEntities(): Generator
    {
        for ($id = 1; $id <= 10000; $id++) {
            $entity = new Entity();
            $entity->setPartitionKey('my-partition');
            $entity->setRowKey((string) $id);
            $entity->addProperty('rand1', EdmType::STRING, (string) rand(1, 1000));
            $entity->addProperty('rand2', EdmType::INT32, rand(1, 1000));
            yield $entity;
        }
    }
}
