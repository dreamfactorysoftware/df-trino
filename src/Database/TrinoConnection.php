<?php

namespace DreamFactory\Core\Trino\Database;

use Illuminate\Database\Connection;
use DreamFactory\Core\Trino\Database\Schema\TrinoSchema;

class TrinoConnection extends Connection
{
    protected $odbcConnection;

    /**
     * Override PDO connection with native ODBC connection for queries.
     */
    protected function getOdbcConnection()
    {
        if (!$this->odbcConnection) {
            $config = $this->getConfig();

            // Extract DSN, username, password from config
            $dsn = $config['odbc']['dsn'] ?? 'TrinoSimba'; // fallback DSN name
            $user = $config['odbc']['username'] ?? '';
            $pass = $config['odbc']['password'] ?? '';

            // Connect using native ODBC
            $this->odbcConnection = odbc_connect($dsn, $user, $pass);

            if (!$this->odbcConnection) {
                throw new \Exception('Failed to connect to ODBC DSN: ' . $dsn);
            }
        }

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
}
