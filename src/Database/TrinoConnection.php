<?php

namespace DreamFactory\Core\Trino\Database;

use Illuminate\Database\Connection;
use DreamFactory\Core\Trino\Database\Schema\TrinoSchema;
use \Illuminate\Database\Query\Builder;
use \Illuminate\Database\Query\Expression;

class TrinoConnection extends Connection
{
    protected $odbcConnection;

    public function __construct($odbcConnection, $database = '', $tablePrefix = '', array $config = [])
    {
        parent::__construct($odbcConnection, $database, $tablePrefix, $config);
        $this->odbcConnection = $odbcConnection;
    }

    /**
     * Return the provided ODBC connection resource.
     */
    protected function getOdbcConnection()
    {
        return $this->odbcConnection;
    }

    /**
     * Override select method to use native ODBC functions to avoid PDO_ODBC binding issues.
     */
    public function select($query, $bindings = [], $useReadPdo = true)
    {
        if ($this->pretending()) {
            return [];
        }

        $conn = $this->getOdbcConnection();

        // Inject bindings directly into the query (very basic, for numbers and strings)
        if (!empty($bindings)) {
            foreach ($bindings as $binding) {
                // Properly quote strings, leave numbers as is
                if (is_numeric($binding)) {
                    $value = $binding;
                } else {
                    // Escape single quotes for SQL
                    $value = "'" . str_replace("'", "''", $binding) . "'";
                }
                // Replace the first occurrence of ? with the value
                $query = preg_replace('/\\?/', $value, $query, 1);
            }
        }

        \Log::debug(['$query' => $query]);
        // No parameter binding support here; bindings must be injected safely before calling select
        $result = odbc_exec($conn, $query);

        if (!$result) {
            $error = odbc_errormsg($conn);
            throw new \Exception("ODBC query failed: $error");
        }

        $rows = [];
        while ($row = odbc_fetch_array($result)) {
            $rows[] = $row;
        }

        odbc_free_result($result);

        return $rows;
    }

    /**
     * Close ODBC connection on destruct.
     */
    public function __destruct()
    {
        if ($this->odbcConnection) {
            odbc_close($this->odbcConnection);
        }
    }

    /**
     * Properly quote a Trino table name: catalog.schema.table (table may contain dots/colons)
     */
    protected function trinoQuoteTableName($fullName)
    {
        $parts = explode('.', $fullName, 3);
        if (count($parts) < 3) {
            throw new \Exception("Invalid Trino table name: $fullName");
        }
        list($catalog, $schema, $table) = $parts;
        return '"' . $catalog . '"."' . $schema . '"."' . $table . '"';
    }

    public function table($table, $as = null)
    {
        $processor = $this->getPostProcessor();
        $grammar = $this->getQueryGrammar();
        $query = new Builder($this, $grammar, $processor);

        // If the table name is a string with at least two dots, quote as Trino expects
        if (is_string($table) && substr_count($table, '.') >= 2) {
            $table = new Expression($this->trinoQuoteTableName($table));
        }

        \Log::debug(['$table' => $table]);
        return $query->from($table, $as);
    }
}
