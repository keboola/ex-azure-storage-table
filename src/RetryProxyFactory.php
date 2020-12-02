<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use GuzzleHttp\Exception\TransferException;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

class RetryProxyFactory
{
    private Config $config;

    private LoggerInterface $logger;

    public function __construct(Config $config, LoggerInterface $logger)
    {
        $this->config = $config;
        $this->logger = $logger;
    }

    public function create(): RetryProxy
    {
        $retryPolicy = new SimpleRetryPolicy(
            $this->config->getMaxTries(),
            [ServiceException::class, TransferException::class]
        );
        $backOffPolicy = new ExponentialBackOffPolicy();
        return new RetryProxy($retryPolicy, $backOffPolicy, $this->logger);
    }
}
