<?php

namespace DreamFactory\Core\Trino\Services;

use DreamFactory\Core\SqlDb\Services\SqlDb;
use Illuminate\Support\Facades\Request;
use DreamFactory\Core\SqlDb\Resources\Table;
use DreamFactory\Core\Trino\Resources\TrinoTable;
use DreamFactory\Core\Enums\ApiOptions;

/**
 * Class TrinoService
 *
 * @package DreamFactory\Core\Trino\Services
 */
class TrinoService extends SqlDb
{
    public function __construct($settings = [])
    {
        parent::__construct($settings);

        $prefix = parent::getConfigBasedCachePrefix();
        $this->setConfigBasedCachePrefix($prefix);
    }

    public function getResourceHandlers()
    {
        $handlers = parent::getResourceHandlers();

        $handlers[Table::RESOURCE_NAME]['class_name'] = TrinoTable::class;

        return $handlers;
    }

    public static function adaptConfig(array &$config)
    {
        $catalog = Request::header('catalog');
        $schema = Request::header('schema');
        if (!empty($catalog) && !empty($schema)) {
            $config['catalog'] = $catalog;
            $config['schema'] = $schema;
        } elseif (!empty($catalog) && empty($schema)) {
            throw new \Exception("If catalog is specified, the schema field cannot be empty.");
        } elseif (empty($catalog) && !empty($schema)) {
            throw new \Exception("If schema is specified, the catalog field cannot be empty.");
        }
        $config['driver'] = 'trino';
        if (!isset($config['odbc'])) {
            $config['odbc'] = [];
        }
        foreach ($config as $key => $value) {
            if (!isset($config['odbc'][$key])) {
                $config['odbc'][$key] = $value;
            }
        }

        parent::adaptConfig($config);
    }

    public static function getDriverName()
    {
        return 'trino';
    }

    public function getApiDocInfo()
    {
        $base = parent::getApiDocInfo();

        //For now, only keep the _table endpoint with GET requests to prove concept
        $paths = [];
        if (isset($base['paths'])) {
            foreach ($base['paths'] as $path => $methods) {
                if (str_contains($path, '_table')) {
                    if (isset($methods['parameters'])) {
                        $paths[$path]['parameters'] = $methods['parameters'];
                    }
                    if (isset($methods['get'])) {
                        $paths[$path]['get'] = $methods['get'];
                        array_push(
                            $paths[$path]['get']['parameters'],
                            $this->getHeaderParam(
                                'catalog',
                                'The name of the catalog to query.' .
                                    'A catalog in Trino is a namespace that contains one or more schemas,' .
                                    'and it represents a specific data source (e.g., a database).'
                            ),
                            $this->getHeaderParam(
                                'schema',
                                'The name of the schema within the specified catalog. ' .
                                    'A schema organizes tables and other database objects,' .
                                    'allowing for better structure and management of the data within the catalog.'
                            )
                        );
                    }
                }
            }
        }
        $base['paths'] = $paths;

        $base['description'] = 'Trino service for connecting to Trino SQL endpoints.';
        return $base;
    }

    private function getHeaderParam($name, $description = null, $required = false): array
    {
        return [
            "name" => $name,
            "description" => $description ?: ucfirst($name) . " for database connection.",
            "schema" => [
                "type" => "string"
            ],
            "in" => "header",
            "required" => $required
        ];
    }
}
