<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use GuzzleHttp\Exception\TransferException;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Promise\PromiseInterface;
use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\CsvWriter\CsvWriterFactory;
use Keboola\AzureStorageTableExtractor\Exception\UserException;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Table\Internal\ITable;
use MicrosoftAzure\Storage\Table\Models\QueryEntitiesOptions;
use MicrosoftAzure\Storage\Table\Models\QueryEntitiesResult;
use Psr\Log\LoggerInterface;
use Retry\RetryProxy;

class Extractor
{
    public const ACCEPT_HEADER = 'application/json;odata=fullmetadata';

    public const PROGRESS_LOG_INTERVAL_SEC = 30;

    private Config $config;

    private LoggerInterface $logger;

    private ITable $tableClient;

    private QueryFactory $queryFactory;

    private CsvWriterFactory $csvWriterFactory;

    private IncrementalFetchingHelper $incFetchingHelper;

    private RetryProxyFactory $retryProxyFactory;

    private RetryProxy $retryProxy;

    private int $pageCount = 0;

    private int $rowsCount = 0;

    private ?float $lastProgressLog = null;

    public function __construct(
        Config $config,
        LoggerInterface $logger,
        ITable $tableClient,
        QueryFactory $queryFactory,
        CsvWriterFactory $csvWriterFactory,
        IncrementalFetchingHelper $incFetchingHelper,
        RetryProxyFactory $retryProxyFactory
    ) {
        $this->config = $config;
        $this->logger = $logger;
        $this->tableClient = $tableClient;
        $this->queryFactory = $queryFactory;
        $this->csvWriterFactory = $csvWriterFactory;
        $this->incFetchingHelper = $incFetchingHelper;
        $this->retryProxyFactory = $retryProxyFactory;
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
        try {
            $this->doExtract();
        } catch (ServiceException|TransferException $e) {
            throw new UserException(
                sprintf('Export of the table "%s" failed: %s', $this->config->getTable(), $e->getMessage()),
                $e->getCode(),
                $e
            );
        }
    }

    private function doExtract(): void
    {
        $this->retryProxy = $this->retryProxyFactory->create();
        $csvWriter = $this->csvWriterFactory->create();
        $limit = $this->config->hasLimit() ? $this->config->getLimit() : null;

        $this->logger->info(sprintf(
            'Exporting table "%s" to "%s" ...',
            $this->config->getTable(),
            $this->config->getOutput()
        ));

        $query = $this->queryFactory->create();
        $options = new QueryEntitiesOptions();
        $options->setQuery($query);
        $options->setDecodeContent(false);
        $options->setAccept(Extractor::ACCEPT_HEADER);

        $prevPagePromise = null;
        while (true) {
            // Wait for the previous page
            /** @var mixed $result -> workaround for phpstan bug */
            $result = $prevPagePromise  ? $this->waitWithRetry($prevPagePromise, $options) : null;

            // Set continuation token if present
            if ($result && $result->getContinuationToken()) {
                $options->setContinuationToken($result->getContinuationToken());
            }

            // Start loading of the the new page, if it is first page, or continuation token is present
            $newPagePromise = !$result || $result->getContinuationToken() ? $this->runQuery($options) : null;

            // Process the previous page, while waiting for the next page ^^^^
            if ($result) {
                $this->pageCount++;
                foreach ($result->getEntities() as &$entity) {
                    $csvWriter->writeItem($entity);
                    $this->rowsCount++;

                    // In the QueryFactory is "$top" set to the limit from the config
                    // But "$top" is limit for one request/page, therefore we must check limit in the code
                    if ($limit && $this->rowsCount >= $limit) {
                        break 2;
                    }
                }

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
        $this->incFetchingHelper->writeState();
    }

    private function waitWithRetry(PromiseInterface $firstPromise, QueryEntitiesOptions $options): QueryEntitiesResult
    {
        $promise = $firstPromise;
        return $this->retryProxy->call(function () use (&$promise, $options) {
            if ($promise->getState() === Promise::REJECTED) {
                $promise = $this->runQuery($options);
            }
            return $promise->wait();
        });
    }

    private function runQuery(QueryEntitiesOptions $options): PromiseInterface
    {
        return $this->tableClient->queryEntitiesAsync($this->config->getTable(), $options);
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
            'Exported "%s" rows / "%s" pages.',
            $this->rowsCount,
            $this->pageCount
        ));
    }
}
