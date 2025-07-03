<?php

namespace DreamFactory\Core\Trino\Models;

use DreamFactory\Core\Models\BaseServiceConfigModel;
use Illuminate\Support\Arr;

/**
 * Class TrinoConfig
 *
 * @package DreamFactory\Core\Trino\Models
 */
class TrinoConfig extends BaseServiceConfigModel
{
    /** @var string */
    protected $table = 'trino_config';

    /** @var array */
    protected $fillable = [
        'service_id',
        'label',
        'description',
        'host',
        'port',
        'username',
        'password',
        'catalog',
        'schema',
        'driver_path',
    ];

    /** @var array */
    protected $casts = [
        'service_id' => 'integer',
    ];

    /**
     * @param array $schema
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);
        switch ($schema['name']) {
            case 'label':
                $schema['label'] = 'Simple label';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] = 'This is just a simple label';
                break;

            case 'description':
                $schema['label'] = 'Description';
                $schema['type'] = 'text';
                $schema['description'] =
                    'This is just a description';
                break;
            case 'host':
                $schema['label'] = 'Trino Host';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] =
                    'Your TrinoService Host URL';
                break;
            case 'port':
                $schema['label'] = 'Trino Port';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] =
                    'Your TrinoService Port';
                break;
            case 'username':
                $schema['label'] = 'Username';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] =
                    'Your TrinoService Username';
                break;
            case 'password':
                $schema['label'] = 'Password';
                $schema['type'] = 'password';
                $schema['required'] = true;
                $schema['description'] =
                    'Your TrinoService Password';
                break;
            case 'driver_path':
                $schema['label'] = 'Trino ODBC Driver Path/Name';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] =
                    'Full path to the ODBC driver on Linux, or registered driver name in curly braces on Windows.';
                $schema['default'] = stripos(PHP_OS, 'WIN') === 0
                    ? '{Simba Trino ODBC Driver}'
                    : '/opt/simba/trinoodbc/lib/64/libtrinoodbc_sb64.so';;
                break;
            case 'catalog':
                $schema['label'] = 'Default Trino Catalog';
                $schema['type'] = 'text';
                $schema['description'] = 'The name of the catalog that will used for requests by default.' .
                    'A catalog in Trino is a namespace that contains one or more schemas and it represents a specific data source.';
                break;
            case 'schema':
                $schema['label'] = 'Default Trino Schema From Catalog';
                $schema['type'] = 'text';
                $schema['description'] =
                    'The name of the schema within the specified catalog that will be used for requests by default. ' .
                    'A schema organizes tables and other database objects,' .
                    'allowing for better structure and management of the data within the catalog.';
                break;
        }
    }
}
