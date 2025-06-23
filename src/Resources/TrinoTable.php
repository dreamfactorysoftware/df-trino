<?php

namespace DreamFactory\Core\Trino\Resources;

use DreamFactory\Core\SqlDb\Resources\Table as SqlDbTable;
use Illuminate\Database\Query\Builder;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\RestException;

class TrinoTable extends SqlDbTable
{
    /**
     * Override runQuery to ensure OFFSET comes before LIMIT for Trino
     */
    protected function runQuery($table, Builder $builder, $extras)
    {
        $schema = $this->parent->getTableSchema($table);
        if (!$schema) {
            throw new NotFoundException("Table '$table' does not exist in the database.");
        }

        $limit = intval(array_get($extras, ApiOptions::LIMIT, 0));
        $offset = intval(array_get($extras, ApiOptions::OFFSET, 0));
        $countOnly = array_get_bool($extras, ApiOptions::COUNT_ONLY);
        $includeCount = array_get_bool($extras, ApiOptions::INCLUDE_COUNT);

        $maxAllowed = $this->getMaxRecordsReturnedLimit();
        $needLimit = false;
        if (($limit < 1) || ($limit > $maxAllowed)) {
            $limit = $maxAllowed;
            $needLimit = true;
        }

        $select = $this->parseSelect($schema, $extras);
        $builder->select($select);

        $order = trim(array_get($extras, ApiOptions::ORDER));
        if (!empty($order)) {
            if (false !== strpos($order, ';')) {
                throw new \DreamFactory\Core\Exceptions\BadRequestException('Invalid order by clause in request.');
            }
            if (stripos($order, 'sleep(') !== false) {
                throw new \DreamFactory\Core\Exceptions\BadRequestException('Use of the sleep() function not supported.');
            }
            $orderComponents = explode(',', $order);
            $processedOrderComponents = [];
            foreach ($orderComponents as $component) {
                $component = trim($component);
                $parts = explode(' ', $component);
                if (count($parts) < 1) {
                    continue;
                }
                $field = $parts[0];
                $direction = 'asc';
                if (isset($parts[1]) && in_array(strtolower($parts[1]), ['asc', 'desc'])) {
                    $direction = $parts[1];
                }
                $nullsOrdering = '';
                if (isset($parts[2]) && strtolower($parts[2]) == 'nulls' && isset($parts[3]) && in_array(strtolower($parts[3]), ['first', 'last'])) {
                    $nullsOrdering = ' NULLS ' . $parts[3];
                }
                $processedOrderComponents[] = $field . ' ' . $direction . $nullsOrdering;
            }
            $processedOrder = implode(', ', $processedOrderComponents);
            $builder->orderByRaw($processedOrder);
        }
        $group = trim(array_get($extras, ApiOptions::GROUP));
        if (!empty($group)) {
            $group = static::fieldsToArray($group);
            $groups = $this->parseGroupBy($schema, $group);
            $builder->groupBy($groups);
        }

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
        // Log for debugging
        \Log::debug(['trino_sql' => $sql, 'bindings' => $bindings]);
        $result = $this->parent->getConnection()->select($sql, $bindings);
        $result = collect($result);

        if (!empty($result)) {
            $related = array_get($extras, ApiOptions::RELATED);
            if (!empty($related) || $schema->fetchRequiresRelations) {
                if (ApiOptions::FIELDS_ALL !== $related) {
                    $related = static::fieldsToArray($related);
                }
                $refresh = array_get_bool($extras, ApiOptions::REFRESH);
                /** @var \DreamFactory\Core\Database\Schema\RelationSchema[] $availableRelations */
                $availableRelations = $schema->getRelations(true);
                if (!empty($availableRelations)) {
                    $data = $result->toArray();
                    $this->retrieveRelatedRecords($schema, $availableRelations, $related, $data, $refresh);
                    $result = collect($data);
                }
            }
        }

        $meta = [];

        if (array_get_bool($extras, ApiOptions::INCLUDE_SCHEMA)) {
            try {
                $meta['schema'] = $schema->toArray(true);
            } catch (RestException $ex) {
                throw $ex;
            } catch (\Exception $ex) {
                throw new InternalServerErrorException("Error describing database table '$table'.\n" .
                    $ex->getMessage(), $ex->getCode());
            }
        }

        $data = $result->toArray();
        if (!empty($meta)) {
            $data['meta'] = $meta;
        }

        return $data;
    }
} 