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

    }

    public function createODBCConnection($dsn)
    {

    }

    protected function getDsn(array $config)
    {

    }
}
