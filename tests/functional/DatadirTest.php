<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\FunctionalTests;

use Throwable;
use Keboola\Component\JsonHelper;
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
        // Remove timestamps from output.csv files (in raw mode)
        $finder = new Finder();
        foreach ($finder->files()->in($tempDatadir . '/out/tables')->name('*.csv') as $file) {
            $content = (string) file_get_contents($file->getPathname());
            $content = (string) preg_replace(
                '~""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z""~',
                '""***""',
                $content
            );
            file_put_contents($file->getPathname(), $content);
        }

        // Pretty print manifests
        $finder = new Finder();
        foreach ($finder->files()->in($tempDatadir . '/out/tables')->name('*.csv.manifest') as $file) {
            $content = (string) file_get_contents($file->getPathname());
            $json = JsonHelper::decode($content);
            file_put_contents($file->getPathname(), JsonHelper::encode($json, true));
        }

        parent::assertMatchesSpecification($specification, $runProcess, $tempDatadir);
    }
}
