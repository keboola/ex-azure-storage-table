<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Tests;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use Keboola\AzureStorageTableExtractor\Configuration\ConfigDefinition;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigTest extends TestCase
{
    /**
     * @dataProvider getValidConfigs
     */
    public function testValidConfig(array $input, array $expected): void
    {
        $config = new Config(['parameters' => $input], new ConfigDefinition());
        Assert::assertSame($expected, $this->configToArray($config));
    }

    /**
     * @dataProvider getInvalidConfigs
     */
    public function testInvalidConfig(string $expectedMsg, array $input): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectDeprecationMessage($expectedMsg);
        new Config(['parameters' => $input], new ConfigDefinition());
    }

    public function getValidConfigs(): iterable
    {
        yield 'minimal' => [
            [
                'db' => $this->getDbNode(),
                'table' => 'input-table',
                'output' => 'output-csv',
                'mode' => 'raw',
            ],
            [
                'connectionString' => 'DefaultEndpointsProtocol=https;...',
                'table' => 'input-table',
                'output' => 'output-csv',
                'maxTries' => 5,
                'select' => null,
                'limit' => null,
                'query' => null,
                'mode' => 'raw',
                'mapping' => null,
                'isIncremental' => false,
                'incrementalFetchingKey' => null,
            ],
        ];

        yield 'generated-query' => [
            [
                'db' => $this->getDbNode(),
                'table' => 'input-table',
                'output' => 'output-csv',
                'maxTries' => 3,
                'select' => 'x.name, x.data',
                'sort' => 'x.date',
                'limit' => 500,
                'mode' => 'raw',
                'incremental' => true,
            ],
            [
                'connectionString' => 'DefaultEndpointsProtocol=https;...',
                'table' => 'input-table',
                'output' => 'output-csv',
                'maxTries' => 3,
                'select' => 'x.name, x.data',
                'limit' => 500,
                'query' => null,
                'mode' => 'raw',
                'mapping' => null,
                'isIncremental' => true,
                'incrementalFetchingKey' => null,
            ],
        ];

        yield 'custom-query' => [
            [
                'db' => $this->getDbNode(),
                'table' => 'input-table',
                'output' => 'output-csv',
                'query' => 'SELECT name, data FROM c',
                'mode' => 'raw',
            ],
            [
                'connectionString' => 'DefaultEndpointsProtocol=https;...',
                'table' => 'input-table',
                'output' => 'output-csv',
                'maxTries' => 5,
                'select' => null,
                'limit' => null,
                'query' => 'SELECT name, data FROM c',
                'mode' => 'raw',
                'mapping' => null,
                'isIncremental' => false,
                'incrementalFetchingKey' => null,
            ],
        ];

        yield 'mapping' => [
            [
                'db' => $this->getDbNode(),
                'table' => 'input-table',
                'output' => 'output-csv',
                'mode' => 'mapping',
                'mapping' => [
                    'id' => [
                        'type' => 'column',
                        'mapping' => [
                            'destination' => 'id',
                            'primaryKey' => true,
                        ],
                    ],
                ],
            ],
            [
                'connectionString' => 'DefaultEndpointsProtocol=https;...',
                'table' => 'input-table',
                'output' => 'output-csv',
                'maxTries' => 5,
                'select' => null,
                'limit' => null,
                'query' => null,
                'mode' => 'mapping',
                'mapping' => [
                    'id' => [
                        'type' => 'column',
                        'mapping' => [
                            'destination' => 'id',
                            'primaryKey' => true,
                        ],
                    ],
                ],
                'isIncremental' => false,
                'incrementalFetchingKey' => null,
            ],
        ];
    }

    public function getInvalidConfigs(): iterable
    {
        yield 'empty' => [
            'The child node "db" at path "root.parameters" must be configured.',
            [],
        ];

        yield 'missing-mapping' => [
            'Invalid configuration, missing "mapping" key, mode is set to "mapping".',
            [
                'db' => $this->getDbNode(),
                'table' => 'input-table',
                'output' => 'output-csv',
                'mode' => 'mapping',
            ],
        ];

        yield 'unexpected-mapping' => [
            'Invalid configuration, "mapping" is configured, but mode is set to "raw".',
            [
                'db' => $this->getDbNode(),
                'table' => 'input-table',
                'output' => 'output-csv',
                'mode' => 'raw',
                'mapping' => ['abc' => 'def'],
            ],
        ];

        yield 'query-and-select' => [
            'Invalid configuration, "query" cannot be configured together with "select".',
            [
                'db' => $this->getDbNode(),
                'table' => 'input-table',
                'output' => 'output-csv',
                'mode' => 'raw',
                'query' => 'SELECT name, data FROM c',
                'select' => 'name, data',
            ],
        ];
    }

    private function configToArray(Config $config): array
    {
        return [
            'connectionString' => $config->getConnectionString(),
            'table' => $config->getTable(),
            'output' => $config->getOutput(),
            'maxTries' => $config->getMaxTries(),
            'select' => $config->hasSelect() ? $config->getSelect() : null,
            'limit' => $config->hasLimit() ? $config->getLimit() : null,
            'query' => $config->hasQuery() ? $config->getQuery() : null,
            'mode' => $config->getMode(),
            'mapping' => $config->getMode() === ConfigDefinition::MODE_MAPPING ? $config->getMapping() : null,
            'isIncremental' => $config->isIncremental(),
            'incrementalFetchingKey' =>
                $config->hasIncrementalFetchingKey() ? $config->getIncrementalFetchingKey() : null,
        ];
    }

    private function getDbNode(): array
    {
        return [
            '#connectionString' => 'DefaultEndpointsProtocol=https;...',
        ];
    }
}
