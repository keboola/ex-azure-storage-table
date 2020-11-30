<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Configuration\Node;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class DbNode extends ArrayNodeDefinition
{
    public const NODE_NAME = 'db';

    public function __construct()
    {
        parent::__construct(self::NODE_NAME);
        $this->isRequired();
        $this->init($this->children());
    }

    protected function init(NodeBuilder $builder): void
    {
        $builder
            ->scalarNode('#connectionString')->isRequired()->cannotBeEmpty()->end();
    }
}
