<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Exception;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use RuntimeException;

class ApplicationException extends RuntimeException implements ApplicationExceptionInterface
{

}
