<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use _HumbugBoxf99c1794c57d\Psr\Log\LogLevel;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use Keboola\AzureStorageTableExtractor\Exception\UserException;
use MicrosoftAzure\Storage\Common\Internal\Authentication\SharedAccessSignatureAuthScheme;
use MicrosoftAzure\Storage\Common\Internal\Middlewares\CommonRequestMiddleware;
use MicrosoftAzure\Storage\Common\Internal\StorageServiceSettings;
use MicrosoftAzure\Storage\Common\Internal\Utilities;
use MicrosoftAzure\Storage\Table\Internal\Authentication\TableSharedKeyLiteAuthScheme;
use MicrosoftAzure\Storage\Table\Internal\MimeReaderWriter;
use MicrosoftAzure\Storage\Table\Internal\TableResources as Resources;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Keboola\AzureStorageTableExtractor\Configuration\Config;
use MicrosoftAzure\Storage\Table\Internal\ITable;
use MicrosoftAzure\Storage\Table\TableRestProxy;

class TableClientFactory
{
    private Config $config;

    private LoggerInterface $logger;

    public function __construct(Config $config, LoggerInterface $logger)
    {
        $this->config = $config;
        $this->logger = $logger;
    }

    public function create(): ITable
    {
        try {
            $options = [
                'middlewares' => [
                    Middleware::log(
                        $this->logger,
                        new MessageFormatter('{method} - {uri} - {code}'),
                        LogLevel::DEBUG
                    ),
                ],
            ];
            return $this->createTableService($this->config->getConnectionString(), $options);
        } catch (RuntimeException $e) {
            throw UserException::from($e, $this->config->getConnectionString(), 'Connection error: ');
        }
    }

    /**
     * Modified version of MicrosoftAzure\Storage\Table\TableRestProxy
     * We are using own $odataSerializer
     */
    private function createTableService(
        string $connectionString,
        array $options = []
    ): ITable {
        $settings = StorageServiceSettings::createFromConnectionString(
            $connectionString
        );

        $odataSerializer = new JsonDeserializer();
        $mimeSerializer = new MimeReaderWriter();

        $primaryUri = Utilities::tryAddUrlScheme(
            $settings->getTableEndpointUri()
        );
        $secondaryUri = Utilities::tryAddUrlScheme(
            $settings->getTableSecondaryEndpointUri()
        );

        $tableWrapper = new TableRestProxy(
            $primaryUri,
            $secondaryUri,
            $odataSerializer,
            $mimeSerializer,
            $options
        );

        // Adding headers filter
        $headers               = array();
        $headers[Resources::DATA_SERVICE_VERSION]     = Resources::DATA_SERVICE_VERSION_VALUE;
        $headers[Resources::MAX_DATA_SERVICE_VERSION] = Resources::MAX_DATA_SERVICE_VERSION_VALUE;
        $headers[Resources::ACCEPT_HEADER]            = Extractor::ACCEPT_HEADER;
        $headers[Resources::ACCEPT_CHARSET]           = Resources::ACCEPT_CHARSET_VALUE;

        // Getting authentication scheme
        if ($settings->hasSasToken()) {
            $authScheme = new SharedAccessSignatureAuthScheme(
                $settings->getSasToken()
            );
        } else {
            $authScheme = new TableSharedKeyLiteAuthScheme(
                $settings->getName(),
                $settings->getKey()
            );
        }

        // Adding common request middleware
        $commonRequestMiddleware = new CommonRequestMiddleware(
            $authScheme,
            Resources::STORAGE_API_LATEST_VERSION,
            Resources::TABLE_SDK_VERSION,
            $headers
        );
        $tableWrapper->pushMiddleware($commonRequestMiddleware);

        return $tableWrapper;
    }
}
