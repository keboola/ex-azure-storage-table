<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\CsvWriter;

use Keboola\AzureStorageTableExtractor\Exception\ApplicationException;
use Keboola\AzureStorageTableExtractor\Exception\UserException;
use Keboola\Component\Manifest\ManifestManager\Options\OutTableManifestOptions;
use Keboola\CsvMap\Exception\CsvMapperException;
use Keboola\CsvMap\Mapper;
use Keboola\CsvTable\Table;

class MappingCsvWriter extends BaseCsvWriter implements ICsvWriter
{
    private Mapper $mapper;

    protected function init(): void
    {
        try {
            $this->mapper = new Mapper($this->config->getMapping(), false, $this->config->getOutput());
        } catch (CsvMapperException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
    }

    public function writeItem(object $item): void
    {
        parent::writeItem($item);
        $this->unsetODataMetadata($item);

        // Ensure UNIQUE FK for sub-documents with the SAME CONTENT, but from the DIFFERENT parent document
        $userData = ['parentId' => md5(serialize($item))];
        try {
            $this->mapper->parseRow($item, $userData);
        } catch (CsvMapperException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
    }

    public function finalize(): void
    {
        $this->copyTempCsvFiles();
        $this->writeManifest();
    }

    protected function copyTempCsvFiles(): void
    {
        foreach ($this->mapper->getCsvFiles() as $csvTable) {
            /** @var Table|null $csvTable */
            if (!$csvTable) {
                // Skip, no row
                continue;
            }

            // Check file size
            $source = $csvTable->getPathName();
            $dest = $this->getCsvTargetPath($csvTable);
            $filesize = filesize($source);
            if ($filesize === false) {
                throw new ApplicationException(sprintf('Failed to get file size "%s".', $source));
            } elseif ($filesize === 0) {
                // No rows -> no CSV file
                continue;
            }

            // Copy
            $result = copy($source, $dest);
            if (!$result) {
                throw new ApplicationException(sprintf('Failed to copy "%s" -> "%s".', $source, $dest));
            }
        }
    }

    protected function writeManifest(): void
    {
        foreach ($this->mapper->getCsvFiles() as $csvTable) {
            /** @var Table|null $csvTable */
            if (!$csvTable) {
                // Skip, no row
                continue;
            }

            // Check if CSV exists
            $csvPath = $this->getCsvTargetPath($csvTable);
            if (!file_exists($csvPath)) {
                // The empty file is not copied, so we also do not create the manifest
                return;
            }

            $this->manifestManager->writeTableManifest(basename($csvPath), $this->getManifest($csvTable));
        }
    }

    protected function getManifest(Table $csvTable): OutTableManifestOptions
    {
        /** @var string[] $primaryKey */
        $primaryKey = $csvTable->getPrimaryKey(true) ?? [];
        $options = new OutTableManifestOptions();
        $options->setColumns($csvTable->getHeader());
        $options->setPrimaryKeyColumns($primaryKey);
        $options->setIncremental($this->config->isIncremental());
        return $options;
    }

    protected function getCsvTargetPath(Table $csvTable): string
    {
        return sprintf('%s/out/tables/%s.csv', $this->dataDir, $csvTable->getName());
    }
}
