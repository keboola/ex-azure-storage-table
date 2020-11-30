<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Configuration;

use Keboola\AzureStorageTableExtractor\Configuration\Node\DbNode;
use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    public const DEFAULT_MAX_TRIES = 5;

    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        $parametersNode->isRequired();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->ignoreExtraKeys(true)
            ->children()
                ->append(new DbNode())
                ->scalarNode('table')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('output')->isRequired()->cannotBeEmpty()->end()
                ->integerNode('maxTries')->min(1)->defaultValue(self::DEFAULT_MAX_TRIES)->end()
                // Custom query
                ->scalarNode('query')->defaultNull()->cannotBeEmpty()->end()
                  // Incremental loading
                ->booleanNode('incremental')->defaultValue(false)->end()
                // Incremental fetching
                ->scalarNode('incrementalFetchingColumn')->defaultNull()->end();
        // @formatter:on
        return $parametersNode;
    }
}
