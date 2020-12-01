<?php

declare(strict_types=1);

require __DIR__ . '/../../../vendor/autoload.php';

use Keboola\AzureStorageTableExtractor\Tests\Data\DataUploader;

$dataUploader = new DataUploader();
$dataUploader->uploadFromDir(__DIR__ . '/tables');
//$dataUploader->uploadBigTable();
