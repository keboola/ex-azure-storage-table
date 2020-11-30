<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Configuration;

use Keboola\AzureStorageTableExtractor\Configuration\Node\DbNode;
use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        $parametersNode->isRequired();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->ignoreExtraKeys(true)
            ->children()
            ->append(new DbNode());
        // @formatter:on
        return $parametersNode;
    }
}
