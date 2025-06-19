<?php

namespace DreamFactory\Core\Trino\Database\Connectors;

use DreamFactory\Core\Trino\Database\Schema\TrinoSchema;
use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use PDO;

class TrinoConnector extends Connector implements ConnectorInterface
{
    public function connect(array $config)
    {
        return $this->createConnection(
            $this->getDsn($config),
            $config,
            $this->getOptions($config)
        );
    }

    protected function getDsn(array $config)
    {
        $cfg = $config['odbc'];

        return "odbc:Driver={$cfg['driver_path']};"
            . "Host={$cfg['host']};"
            . "Port={$cfg['port']};"
            . "AuthMech=0;"
            . "UID={$cfg['username']};"
            . "PWD={$cfg['password']}";
    }
}
