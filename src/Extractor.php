<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use GuzzleHttp\Promise\Promise;
use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\CsvWriter\CsvWriterFactory;
use Keboola\AzureStorageTableExtractor\Exception\UserException;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Table\Internal\ITable;
use MicrosoftAzure\Storage\Table\Models\QueryEntitiesOptions;
use Psr\Log\LoggerInterface;

class Extractor
{
    public const ACCEPT_HEADER = 'application/json;odata=nometadata';

    public const PROGRESS_LOG_INTERVAL_SEC = 30;

    private Config $config;

    private LoggerInterface $logger;

    private ITable $tableClient;

    private QueryFactory $queryFactory;

    private CsvWriterFactory $csvWriterFactory;

    private array $inputState;

    private int $pageCount = 0;

    private int $rowsCount = 0;

    private ?float $lastProgressLog = null;

    public function __construct(
        Config $config,
        LoggerInterface $logger,
        ITable $tableClient,
        QueryFactory $queryFactory,
        CsvWriterFactory $csvWriterFactory,
        array $inputState
    ) {
        $this->config = $config;
        $this->logger = $logger;
        $this->tableClient = $tableClient;
        $this->queryFactory = $queryFactory;
        $this->csvWriterFactory = $csvWriterFactory;
        $this->inputState = $inputState;
    }

    public function testConnection(): void
    {
        try {
            $this->tableClient->queryTables();
        } catch (ServiceException $e) {
            throw UserException::from($e, $this->config->getConnectionString());
        }
    }

    public function extract(): void
    {
        $csvWriter = $this->csvWriterFactory->create();

        $query = $this->queryFactory->create();
        $options = new QueryEntitiesOptions();
        $options->setQuery($query);
        $options->setDecodeContent(false);
        $options->setAccept(Extractor::ACCEPT_HEADER);

        $this->logger->info(sprintf(
            'Exporting table "%s" to "%s" ...',
            $this->config->getTable(),
            $this->config->getOutput()
        ));

        /** @var Promise|null $prevPagePromise */
        $prevPagePromise = null;
        while (true) {
            // Wait for the previous page
            $result = $prevPagePromise ? $prevPagePromise->wait() : null;

            // Set continuation token if present
            if ($result && $result->getContinuationToken()) {
                $options->setContinuationToken($result->getContinuationToken());
            }

            // Start loading of the the new page, if it is first page, or continuation token is present
            $newPagePromise = !$result || $result->getContinuationToken() ?
                $this->tableClient->queryEntitiesAsync($this->config->getTable(), $options) : null;

            // Process the previous page, while waiting for the next page ^^^^
            if ($result) {
                foreach ($result->getEntities() as &$entity) {
                    $csvWriter->writeItem($entity);
                    $this->rowsCount++;
                }
                $this->pageCount++;
                $this->logProgress();
            }

            // No more pages?
            if (!$newPagePromise) {
                break;
            }
            $prevPagePromise = $newPagePromise;
        }

        $this->logFinalStats();

        // All items wrote, finalize
        $csvWriter->finalize();

        // Write last state incremental fetching
        if ($this->config->hasIncrementalFetchingKey()) {
            $csvWriter->writeLastState($this->inputState);
        }
    }

    private function logProgress(): void
    {
        if (microtime(true) - $this->lastProgressLog < self::PROGRESS_LOG_INTERVAL_SEC) {
            return;
        }

        if ($this->lastProgressLog) {
            $this->logger->info(sprintf(
                'Progress: "%s" rows / "%s" pages exported.',
                $this->rowsCount,
                $this->pageCount
            ));
        }

        $this->lastProgressLog = microtime(true);
    }

    private function logFinalStats(): void
    {
        $this->logger->info(sprintf(
            'Exported all "%s" rows / "%s" pages.',
            $this->rowsCount,
            $this->pageCount
        ));
    }
}
