<?php

namespace DreamFactory\Core\Trino\Database\Schema;

use DreamFactory\Core\SqlDb\Database\Schema\SqlSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Database\Schema\ColumnSchema;

class TrinoSchema extends SqlSchema
{
    public function getSupportedResourceTypes()
    {
        return [
            DbResourceTypes::TYPE_TABLE,
            DbResourceTypes::TYPE_TABLE_FIELD,
            DbResourceTypes::TYPE_TABLE_CONSTRAINT,
            DbResourceTypes::TYPE_TABLE_RELATIONSHIP,
        ];
    }

    protected function getTableNames($schema = '', $catalog = '')
    {
        $catalog = $catalog ?: $this->getDefaultCatalog();
        if (empty($catalog)) {
            throw new \Exception('No available catalogs found');
        }
        $sql = 'SHOW TABLES FROM ' . $catalog;

        if (!empty($schema)) {
            $sql .= ".{$schema}";
        } else {
            $schema = $this->getDefaultSchema($catalog);
            $sql .= ".{$schema}";
        }

        \Log::debug($sql);
        $rows = $this->connection->select($sql);

        $names = [];
        foreach ($rows as $row) {
            $row = array_values((array)$row);
            $catalogName = $catalog;
            $schemaName = $schema;
            $resourceName = $row[0];
            $internalName = $catalogName . '.' . $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
            $settings = compact('catalogName', 'schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

    /**
     * Get schema names for the current or specified catalog
     *
     * @param string $catalog
     * @return array
     */
    public function getSchemas($catalog = '')
    {
        \Log::debug('getSchemas triggered');
        $catalog = $catalog ?: $this->getDefaultCatalog();
        if (empty($catalog)) {
            throw new \Exception('No catalogs found for schema discovery');
        }

        $sql = "select schema_name FROM {$catalog}.information_schema.schemata";
        \Log::debug($sql);

        $rows = $this->connection->select($sql);

        $schemas = [];
        foreach ($rows as $row) {
            $row = array_values((array)$row);
            $schemas[] = $row[0];
        }

        return $schemas;
    }

    /**
     * Get available catalogs
     *
     * @return array
     */
    public function getCatalogs()
    {
        // Use standard SQL instead of SHOW command for better ODBC compatibility
        $sql = "SHOW CATALOGS";

        $rows = $this->connection->select($sql);

        $catalogs = [];
        foreach ($rows as $row) {
            $row = array_values((array)$row);
            $catalogs[] = $row[0];
        }

        return $catalogs;
    }

    /**
     * Get default catalog (first available catalog)
     *
     * @return string|null
     */
    public function getDefaultCatalog()
    {
        $specifiedCatalog = $this->connection->getConfig('catalog');
        $fallbackCatalogs = $this->getCatalogs();

        if (empty($specifiedCatalog)) {
            return !empty($fallbackCatalogs) ? $fallbackCatalogs[0] : null;
        } else {
            return $specifiedCatalog;
        }

        return null;
    }

    /**
     * Get default schema for a given or current catalog
     *
     * @param string $catalog
     * @return string|null
     */
    public function getDefaultSchema($catalog = '')
    {
        $specifiedSchema = $this->connection->getConfig('schema');
        $fallbackSchemas = $this->getSchemas($catalog);

        \Log::debug(['specifiedSchema' => $specifiedSchema]);
        if (empty($specifiedSchema)) {
            return !empty($fallbackSchemas) ? $fallbackSchemas[0] : null;
        } else {
            return $specifiedSchema;
        }

        return null;
    }
}
