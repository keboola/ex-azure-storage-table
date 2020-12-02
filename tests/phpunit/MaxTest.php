<?php

declare(strict_types=1);

namespace Keboola\AzureStorageTableExtractor\Tests;

use Keboola\AzureStorageTableExtractor\IncrementalFetchingHelper;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class MaxTest extends TestCase
{
    /**
     * @dataProvider getData
     * @param mixed $value1
     * @param mixed $value2
     * @param mixed $expected
     */
    public function testMax($value1, $value2, $expected): void
    {
        Assert::assertSame($expected, IncrementalFetchingHelper::max($value1, $value2));
        Assert::assertSame($expected, IncrementalFetchingHelper::max($value2, $value1));
    }

    public function getData(): iterable
    {
        // value1, value2, max
        yield [-100, -100, -100];
        yield [-100.0, -100.0, -100.0];
        yield ['-100', '-100', '-100'];
        yield ['-100.0', '-100.0', '-100.0'];
        yield [-100, -99, -99];
        yield [-100.0, -99.0, -99.0];
        yield [-99.2, -99.1, -99.1];
        yield ['-100', '-99', '-99'];
        yield ['-100.0', '-99.0', '-99.0'];
        yield [0, 0, 0];
        yield [0.0, 0.0, 0.0];
        yield ['0', '0', '0'];
        yield ['0.0', '0.0', '0.0'];
        yield [0, 100, 100];
        yield [100.0, 100.0, 100.0];
        yield ['0', '100', '100'];
        yield ['0.0', '100.0', '100.0'];
        yield [9999, 10000, 10000];
        yield [9999.0, 10000.0, 10000.0];
        yield ['9999', '10000', '10000'];
        yield ['9999.0', '10000.0', '10000.0'];
        yield ['abc', 'abc', 'abc'];
        yield ['abc', 'Abc', 'abc'];
        yield ['abc', 'def', 'def'];
        yield ['abc', 'Def', 'abc'];
        yield ['dog', 'dog2', 'dog2'];
        yield ['dog2', 'dog3', 'dog3'];
        yield ['dog2', 'dog3', 'dog3'];
        yield ['2020-01-17T16:07:34', '2020-04-01T08:22:49', '2020-04-01T08:22:49'];
        yield ['2020-01-01T00:00:00', '2020-01-01T00:00:01', '2020-01-01T00:00:01'];
        yield ['2020-01-02T00:00:00', '2020-01-01T00:00:01', '2020-01-02T00:00:00'];
    }
}
