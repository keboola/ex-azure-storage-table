<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use GuzzleHttp\Promise\Promise;
use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\Exception\UserException;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Table\Internal\ITable;
use MicrosoftAzure\Storage\Table\Models\QueryEntitiesOptions;
use MicrosoftAzure\Storage\Table\Models\QueryEntitiesResult;
use Psr\Log\LoggerInterface;

class Extractor
{
    public const ACCEPT_HEADER = 'application/json;odata=nometadata';

    private Config $config;

    private LoggerInterface $logger;

    private ITable $tableClient;

    private QueryFactory $queryFactory;

    private int $pageCount = 0;

    public function __construct(
        Config $config,
        LoggerInterface $logger,
        TableClientFactory $clientFactory,
        QueryFactory $queryFactory
    ) {
        $this->config = $config;
        $this->logger = $logger;
        $this->tableClient = $clientFactory->create();
        $this->queryFactory = $queryFactory;
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
        $query = $this->queryFactory->create();
        $options = new QueryEntitiesOptions();
        $options->setQuery($query);
        $options->setAccept(Extractor::ACCEPT_HEADER);

        $this->logger->info(sprintf(
            'Exporting table "%s" to "%s" ...',
            $this->config->getTable(),
            $this->config->getOutput()
        ));

        /** @var Promise|null $prevPagePromise */
        $prevPagePromise = null;
        while (true) {
            // Start loading of the the new page
            $newPagePromise = $this->tableClient->queryEntitiesAsync($this->config->getTable(), $options);

            // Process the previous page, while waiting for the next page ^^^^
            if ($prevPagePromise) {
                $result = $prevPagePromise->wait();
                $this->writeResultPage($result);

                if ($result->getContinuationToken()) {
                    $options->setContinuationToken($result->getContinuationToken());
                } else {
                    // no more page
                    break;
                }

                $this->pageCount++;
            }

            $prevPagePromise = $newPagePromise;
        }

        $this->finalize();
    }

    private function writeResultPage(QueryEntitiesResult $result): void
    {
        //var_dump($result->getEntities());
    }

    private function finalize(): void
    {
    }
}
