<?php

namespace DreamFactory\Core\Trino\Database\Connectors;

use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;

class TrinoConnector extends Connector implements ConnectorInterface
{
    public function connect(array $config)
    {
        $required = ['username', 'password', 'driver_path', 'host', 'port'];

        foreach ($required as $key) {
            if (empty($config['odbc'][$key])) {
                throw new \InvalidArgumentException("Missing required ODBC config parameter: '$key'");
            }
        }

        $dsn      = 'TrinoSimbaODBC';
        $user     = $config['odbc']['username'];
        $pass     = $config['odbc']['password'];
        $driver_path   = $config['odbc']['driver_path'];
        $host     = $config['odbc']['host'];
        $port     = $config['odbc']['port'];

        try {
            if (stripos(PHP_OS, 'WIN') === 0) {
                // Windows: DSN-less connection string
                $driverEscaped = str_replace('\\', '\\\\', $driver_path);
                $connectionString = "Driver={$driverEscaped};Host={$host};Port={$port};";
            } else {
                // Linux: create a temporary odbc.ini with DSN
                $dsn      = 'TrinoSimbaODBC';
                $tempDir  = sys_get_temp_dir();
                $odbcIni  = $tempDir . DIRECTORY_SEPARATOR . 'dreamfactory_trino_odbc.ini';

                $iniContent = <<<EOT
[$dsn]
Driver=$driver_path
Host=$host
Port=$port
EOT;

                if (@file_put_contents($odbcIni, $iniContent) === false) {
                    throw new \RuntimeException("Failed to write temporary odbc.ini to: $odbcIni");
                }

                if (!putenv("ODBCINI=$odbcIni")) {
                    throw new \RuntimeException("Failed to set ODBCINI environment variable");
                }

                $connectionString = $dsn;
            }

            $connection = odbc_connect($connectionString, $user, $pass);

            if (!$connection) {
                $code = odbc_error();
                $msg  = odbc_errormsg();
                throw new \RuntimeException("ODBC connection failed [{$code}]: {$msg}");
            }

            return $connection;
        } catch (\Throwable $e) {
            throw new \RuntimeException("Trino ODBC connection error: " . $e->getMessage(), 0, $e);
        }
    }
}
