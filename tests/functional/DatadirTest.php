<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;

class DatadirTest extends DatadirTestCase
{
    protected function assertMatchesSpecification(
        DatadirTestSpecificationInterface $specification,
        Process $runProcess,
        string $tempDatadir
    ): void {
        // Remove timestamps from output.csv files
        $finder = new Finder();
        foreach ($finder->files()->in($tempDatadir . '/out/tables')->name('*.csv') as $csvFile) {
            $content = (string) file_get_contents($csvFile->getPathname());
            $content = (string) preg_replace(
                '~\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z~',
                '***',
                $content
            );
            file_put_contents($csvFile->getPathname(), $content);
        }

        parent::assertMatchesSpecification($specification, $runProcess, $tempDatadir);
    }
}
