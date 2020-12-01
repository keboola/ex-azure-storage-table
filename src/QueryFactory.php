<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use Keboola\AzureStorageTableExtractor\Configuration\Config;
use MicrosoftAzure\Storage\Table\Models\Filters\Filter;
use MicrosoftAzure\Storage\Table\Models\Query;

class QueryFactory
{
    private Config $config;

    private IncrementalFetchingHelper $incFetchingHelper;

    public function __construct(Config $config, IncrementalFetchingHelper $incFetchingHelper)
    {
        $this->config = $config;
        $this->incFetchingHelper = $incFetchingHelper;
    }

    public function create(): Query
    {
        $query = new Query();

        // Apply incremental fetching filter
        if ($this->incFetchingHelper->hasValue()) {
            $filter = Filter::applyGe(
                Filter::applyPropertyName($this->incFetchingHelper->getKey()),
                Filter::applyConstant(
                    $this->incFetchingHelper->getValue(),
                    $this->incFetchingHelper->getValueType()
                )
            );
            $query->setFilter($filter);
        }

        return $query;
    }
}
