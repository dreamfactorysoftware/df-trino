<?php
namespace DreamFactory\Core\Trino;

use DreamFactory\Core\Services\ServiceType;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Trino\Models\TrinoConfig;
use DreamFactory\Core\Enums\LicenseLevel;
use DreamFactory\Core\Trino\Database\Connectors\TrinoConnector;
use DreamFactory\Core\Trino\Database\TrinoConnection;
use DreamFactory\Core\Services\ServiceManager;
use Illuminate\Database\DatabaseManager;
use DreamFactory\Core\Trino\Services\TrinoService;
use DreamFactory\Core\Trino\Database\Schema\TrinoSchema;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
 public function boot()
    {
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');
    }

    public function register()
    {
        $this->app->resolving('db', function (DatabaseManager $db) {
            $db->extend('trino', function ($config) {
                $connector = new TrinoConnector();
                $connection = $connector->connect($config);

                return new TrinoConnection($connection, $config["database"], $config["prefix"], $config);
            });
        });

        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'                  => 'trino',
                    'label'                 => 'Trino',
                    'description'           => 'Database service supporting Trino connections.',
                    'group'                 => ServiceTypeGroups::DATABASE,
                    'subscription_required' => LicenseLevel::SILVER, // Tepm value, so needs to be clarified
                    'config_handler'        => TrinoConfig::class,
                    'factory'               => function ($config) {
                        return new TrinoService($config);
                    },
                ])
            );
        });

        $this->app->resolving('df.db.schema', function ($db) {
            /** @var DatabaseManager $db */
            $db->extend('trino', function ($connection) {
                return new TrinoSchema($connection);
            });
        });
    }
}
