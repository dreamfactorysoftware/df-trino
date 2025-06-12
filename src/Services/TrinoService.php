<?php namespace DreamFactory\Core\Trino\Services;
use DreamFactory\Core\SqlDb\Services\SqlDb;


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

    public static function adaptConfig(array &$config)
    {
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
                    if (isset($methods['get'])) {
                        $paths[$path]['get'] = $methods['get'];
                    }
                }
            }
        }
        $base['paths'] = $paths;

        $base['description'] = 'Trino service for connecting to Trino SQL endpoints.';
        return $base;
    }
}
