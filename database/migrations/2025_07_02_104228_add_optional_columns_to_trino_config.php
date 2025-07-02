<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up()
    {
        Schema::table('trino_config', function (Blueprint $table) {
            $table->string('catalog')->nullable();
            $table->string('schema')->nullable();
        });
    }

    public function down()
    {
        Schema::table('trino_config', function (Blueprint $table) {
            $table->dropColumn('catalog');
            $table->dropColumn('schema');
        });
    }
};
