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

        // Filter
        if ($this->incFetchingHelper->hasValue()) {
            $filter = Filter::applyGe(
                Filter::applyPropertyName($this->incFetchingHelper->getKey()),
                Filter::applyConstant(
                    $this->incFetchingHelper->getValue(),
                    $this->incFetchingHelper->getValueType()
                )
            );
            $query->setFilter($filter);
        } elseif ($this->config->hasFilter()) {
            $query->setFilter(Filter::applyQueryString($this->config->getFilter()));
        }

        // Select
        if ($this->config->hasSelect()) {
            $query->setSelectFields($this->config->getSelect());
        }

        // Limit
        if ($this->config->hasLimit()) {
            $query->setTop($this->config->getLimit());
        }

        return $query;
    }
}
