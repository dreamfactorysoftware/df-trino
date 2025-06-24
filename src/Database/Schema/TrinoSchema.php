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
            $tableSchema = new TableSchema($settings);

            $this->loadTableColumns($tableSchema);

            $names[strtolower($name)] = $tableSchema;
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

    /**
     * Load columns for a given table into the TableSchema object.
     *
     * @param TableSchema $table
     */
    protected function loadTableColumns(TableSchema $table)
    {
        $catalog = $table->catalogName ?? $this->getDefaultCatalog();
        $schema = $table->schemaName ?? $this->getDefaultSchema($catalog);
        $tableName = $table->resourceName ?? $table->name;

        $sql = 'SHOW COLUMNS FROM ' . $catalog . '.' . $schema . '.' . $tableName;
        $columns = $this->connection->select($sql);

        foreach ($columns as $column) {
            $column = array_change_key_case((array)$column, CASE_LOWER);
            $c = new ColumnSchema(['name' => $column['column']]);
            $c->quotedName = $this->quoteColumnName($c->name);
            $c->dbType = $column['type'];
            $c->allowNull = (isset($column['null']) ? strtolower($column['null']) === 'yes' : true);
            $c->defaultValue = $column['default'] ?? null;
            $c->isPrimaryKey = false;
            $this->extractType($c, $c->dbType);
            $this->extractLimit($c, $c->dbType);
            $c->fixedLength = $this->extractFixedLength($c->dbType);

            $table->addColumn($c);
        }
    }
}
