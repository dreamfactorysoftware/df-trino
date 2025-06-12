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
                $schema['label'] = 'Trino ODBC Driver Path';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] =
                    'Your ODBC Driver Path';
                break;
        }
    }
}
