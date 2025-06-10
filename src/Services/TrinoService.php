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
        parent::adaptConfig($config);
    }

    public static function getDriverName()
    {
        return 'trino';
    }
}
