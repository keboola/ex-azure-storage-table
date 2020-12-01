<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor;

use GuzzleHttp\Psr7\Stream;
use Keboola\Component\JsonHelper;
use MicrosoftAzure\Storage\Table\Internal\TableResources as Resources;
use MicrosoftAzure\Storage\Table\Models\Entity;
use RuntimeException;
use MicrosoftAzure\Storage\Table\Internal\IODataReaderWriter;

/**
 * Our version of the MicrosoftAzure\Storage\Table\Internal\JsonODataReaderWriter
 * We do not need to map the results to entities.
 */
class JsonDeserializer implements IODataReaderWriter
{

    /** @inheritDoc */
    public function getTable($name): void
    {
        throw new RuntimeException('Not implemented.');
    }

    /** @inheritDoc */
    public function parseTable($body): void
    {
        throw new RuntimeException('Not implemented.');
    }

    /** @inheritDoc */
    public function parseTableEntries($body): array
    {
        // We use "queryTables" to test connection
        // ... but we don't need to parse response.
        // See Extractor class
        return ['not-implemented'];
    }

    /** @inheritDoc */
    public function getEntity(Entity $entity): void
    {
        throw new RuntimeException('Not implemented.');
    }

    /** @inheritDoc */
    public function parseEntity($body): void
    {
        throw new RuntimeException('Not implemented.');
    }

    /** @inheritDoc */
    public function parseEntities($body)
    {
        // Parse JSON body to array
        /** @var Stream $stream */
        $stream = $body;
        $stream->rewind();
        $bodyArray = json_decode($stream->getContents(), false, 512, JSON_THROW_ON_ERROR);
        return $bodyArray->{Resources::JSON_VALUE};
    }
}
