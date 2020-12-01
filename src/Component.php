<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use Keboola\AzureStorageTableExtractor\Configuration\ActionConfigDefinition;
use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\Configuration\ConfigDefinition;
use Keboola\AzureStorageTableExtractor\CsvWriter\CsvWriterFactory;
use Keboola\Component\BaseComponent;
use Psr\Log\LoggerInterface;

class Component extends BaseComponent
{
    public const ACTION_RUN = 'run';
    public const ACTION_TEST_CONNECTION = 'testConnection';

    private Extractor $extractor;

    public function __construct(LoggerInterface $logger)
    {
        parent::__construct($logger);
        $config = $this->getConfig();
        $clientFactory = new TableClientFactory($config);
        $queryFactory = new QueryFactory($config);
        $csvWriterFactory = new CsvWriterFactory($this->getDataDir(), $config);
        $this->extractor = new Extractor(
            $config,
            $this->getLogger(),
            $clientFactory->create(),
            $queryFactory,
            $csvWriterFactory,
            $this->getInputState()
        );
    }

    protected function getSyncActions(): array
    {
        return [
            self::ACTION_TEST_CONNECTION => 'handleTestConnection',
        ];
    }

    protected function run(): void
    {
        $this->extractor->extract();
    }

    protected function handleTestConnection(): array
    {
        $this->extractor->testConnection();
        return ['success' => true];
    }

    public function getConfig(): Config
    {
        /** @var Config $config */
        $config = parent::getConfig();
        return $config;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        $action = $this->getRawConfig()['action'] ?? 'run';
        return $action === 'run' ? ConfigDefinition::class : ActionConfigDefinition::class;
    }
}
