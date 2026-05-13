<?php

namespace DreamFactory\Core\Trino\Tests\Security;

use DreamFactory\Core\Trino\Database\Schema\TrinoSchema;
use PHPUnit\Framework\TestCase;

/**
 * Security: TrinoSchema must validate catalog/schema identifiers since
 * Trino does not allow them to be passed as bound parameters.
 */
class TrinoIdentifierTest extends TestCase
{
    /**
     * @dataProvider validIdentifierProvider
     */
    public function testAcceptsValidIdentifier(string $value): void
    {
        TrinoSchema::assertSafeTrinoIdentifier($value);
        $this->assertTrue(true);
    }

    public static function validIdentifierProvider(): array
    {
        return [
            'lowercase' => ['mycatalog'],
            'with underscore' => ['my_catalog'],
            'with dash' => ['my-catalog'],
            'with digits' => ['cat_1'],
            'leading underscore' => ['_internal'],
        ];
    }

    /**
     * @dataProvider invalidIdentifierProvider
     */
    public function testRejectsInvalidIdentifier(string $value): void
    {
        $this->expectException(\InvalidArgumentException::class);
        TrinoSchema::assertSafeTrinoIdentifier($value);
    }

    public static function invalidIdentifierProvider(): array
    {
        return [
            'with dot'       => ['cat.schema'],
            'with space'     => ['my catalog'],
            'with quote'     => ["cat'name"],
            'with paren'     => ['cat)'],
            'with semicolon' => ['cat; SELECT'],
            'starts digit'   => ['1cat'],
            'empty'          => [''],
            'with newline'   => ["cat\nname"],
        ];
    }
}
