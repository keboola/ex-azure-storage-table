<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Exception;

use Throwable;
use RuntimeException;
use Keboola\CommonExceptions\UserExceptionInterface;

class UserException extends RuntimeException implements UserExceptionInterface
{
    public static function from(Throwable $e, string $connectionString, string $msgPrefix = ''): self
    {
        $msg = $e->getMessage();
        // Hide connection string
        $msg = str_replace($connectionString, '*****', $msg);
        throw new static($msgPrefix . $msg, $e->getCode(), $e);
    }
}
