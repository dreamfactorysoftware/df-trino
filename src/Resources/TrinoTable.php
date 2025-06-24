<?php

namespace DreamFactory\Core\Trino\Resources;

use DreamFactory\Core\SqlDb\Resources\Table;
use Illuminate\Database\Query\Builder;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Database\Schema\TableSchema;

class TrinoTable extends Table
{
    /**
     * Override getQueryResults to ensure OFFSET comes before LIMIT for Trino
     * @param TableSchema $schema
     * @param Builder     $builder
     * @param array       $extras
     * @return Collection
     */
    protected function getQueryResults(TableSchema $schema, Builder $builder, $extras)
    {
        $limit = intval(array_get($extras, ApiOptions::LIMIT, 0));
        $offset = intval(array_get($extras, ApiOptions::OFFSET, 0));
        // Instead of $builder->take($limit)->skip($offset), we build the SQL manually
        $sql = $builder->toSql();
        $bindings = $builder->getBindings();
        // Remove any existing LIMIT/OFFSET
        $sql = preg_replace('/\s+limit\s+\d+/i', '', $sql);
        $sql = preg_replace('/\s+offset\s+\d+/i', '', $sql);
        // Remove any trailing semicolon or whitespace
        $sql = rtrim($sql, " ;");
        // Add OFFSET before LIMIT for Trino, with a space
        if ($offset > 0) {
            $sql .= ' OFFSET ' . $offset;
        }
        if ($limit > 0) {
            $sql .= ' LIMIT ' . $limit;
        }
        $result = $this->parent->getConnection()->select($sql, $bindings);

        return collect($result); 
    }
}
