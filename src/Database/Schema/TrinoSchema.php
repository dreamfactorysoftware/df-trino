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

    protected function getTableNames($schema = null)
    {
        $catalog = $this->getDefaultCatalog();
        if (empty($catalog)) {
            throw new \Exception('No available catalogs found');
        }

        $schema = $this->getDefaultSchema($catalog);
        $sql = 'SHOW TABLES FROM ' . $catalog . '.' . $schema;
        $rows = $this->connection->select($sql);

        $columnsMap = $this->loadAllColumns($catalog, $schema); // Get all columns for the schema

        $names = [];
        foreach ($rows as $row) {
            $row = array_values((array)$row);
            $resourceName = $row[0];
            $internalName = $catalog . '.' . $schema . '.' . $resourceName;
            $quotedName = $this->quoteTableName($schema) . '.' . $this->quoteTableName($resourceName);

            $settings = compact('catalog', 'schema', 'resourceName', 'internalName', 'quotedName');
            $settings['name'] = $resourceName;

            $tableSchema = new TableSchema($settings);

            if (isset($columnsMap[$resourceName])) {
                $this->assignColumnsToTable($tableSchema, $columnsMap[$resourceName]);
            }

            $names[strtolower($resourceName)] = $tableSchema;
        }

        \Log::debug(['$names' => $names]);
        return $names;
    }

    protected function loadAllColumns($catalog, $schema)
    {
        $sql = "SELECT table_name, column_name, data_type, is_nullable, column_default
            FROM {$catalog}.information_schema.columns
            WHERE table_schema = '{$schema}'";

        $rows = $this->connection->select($sql);

        $map = [];
        foreach ($rows as $column) {
            $col = array_change_key_case((array)$column, CASE_LOWER);
            $table = $col['table_name'];
            $map[$table][] = $col;
        }

        \Log::debug(['$map' => $map]);
        return $map;
    }

    protected function assignColumnsToTable(TableSchema $table, array $columns)
    {
        foreach ($columns as $column) {
            $c = new ColumnSchema(['name' => $column['column_name']]);
            $c->quotedName = $this->quoteColumnName($c->name);
            $c->dbType = $column['data_type'];
            $c->allowNull = strtolower($column['is_nullable']) === 'yes';
            $c->defaultValue = $column['column_default'] ?? null;
            $c->isPrimaryKey = false;
            $this->extractType($c, $c->dbType);
            $this->extractLimit($c, $c->dbType);
            $c->fixedLength = $this->extractFixedLength($c->dbType);

            $table->addColumn($c);
        }
    }

    /**
     * Get schema names for the current or specified catalog
     *
     * @param string $catalog
     * @return array
     */
    public function getSchemas($catalog = '')
    {
        $catalog = $catalog ?: $this->getDefaultCatalog();
        if (empty($catalog)) {
            throw new \Exception('No catalogs found for schema discovery');
        }

        $sql = "select schema_name FROM {$catalog}.information_schema.schemata";

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

        if (empty($specifiedSchema)) {
            return !empty($fallbackSchemas) ? $fallbackSchemas[0] : null;
        } else {
            return $specifiedSchema;
        }

        return null;
    }
}
