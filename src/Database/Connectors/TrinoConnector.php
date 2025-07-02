<?php

namespace DreamFactory\Core\Trino\Database\Connectors;

use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;

class TrinoConnector extends Connector implements ConnectorInterface
{
    public function connect(array $config)
    {
        // Required fields for DSN-based ODBC
        $required = ['username', 'password', 'driver_path', 'host', 'port'];

        foreach ($required as $key) {
            if (empty($config['odbc'][$key])) {
                throw new \InvalidArgumentException("Missing required ODBC config parameter: '$key'");
            }
        }

        // Extract config values
        $dsn      = 'TrinoSimbaODBC';
        $user     = $config['odbc']['username'];
        $pass     = $config['odbc']['password'];
        $driver_path   = $config['odbc']['driver_path'];
        $host     = $config['odbc']['host'];
        $port     = $config['odbc']['port'];

        // Temp file paths
        $odbcIni     = '/var/tmp/dreamfactory_trino_dns_odbc.ini';

        // Write odbc.ini
        file_put_contents($odbcIni, <<<EOT
[$dsn]
Driver=$driver_path
Host=$host
Port=$port
EOT
        );

        // Set env variables for unixODBC
        putenv("ODBCINI=$odbcIni");

        // Attempt connection
        $connection = odbc_connect($dsn, $user, $pass);

        if (!$connection) {
            throw new \RuntimeException('ODBC connection failed for DSN:' . $dsn . ' - ' . odbc_errormsg());
        }

        return $connection;
    }
}
