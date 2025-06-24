<?php

namespace DreamFactory\Core\Trino\Database\Connectors;

use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;

class TrinoConnector extends Connector implements ConnectorInterface
{
    public function connect(array $config)
    {
        // Extract DSN, username, password from config
        $dsn = $config['odbc']['dsn'] ?? 'TrinoSimba'; // fallback DSN name
        $user = $config['odbc']['username'] ?? '';
        $pass = $config['odbc']['password'] ?? '';

        $odbcConnection = odbc_connect($dsn, $user, $pass);

        if (!$odbcConnection) {
            throw new \Exception('Failed to connect to ODBC DSN: ' . $dsn);
        }

        return $odbcConnection;
    }
} 