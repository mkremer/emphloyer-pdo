<?php

declare(strict_types=1);

namespace Emphloyer\Pdo;

use Emphloyer\Pipeline\BackendTestCase;
use PDO;

class PipelineBackendTest extends BackendTestCase
{
    public function setUp() : void
    {
        $this->pdo = new PDO($GLOBALS['DB_DSN'], $GLOBALS['DB_USER'], $GLOBALS['DB_PASSWD']);
        $this->pdo->exec('DROP TABLE IF EXISTS test_jobs');
        $this->pdo->exec('CREATE TABLE test_jobs (uuid VARCHAR(36) PRIMARY KEY, created_at TIMESTAMP, run_from TIMESTAMP NULL DEFAULT NULL, locked_at TIMESTAMP NULL DEFAULT NULL, lock_uuid VARCHAR(36) UNIQUE, status VARCHAR(20), class_name VARCHAR(255), type VARCHAR(100), attributes TEXT);');
        $this->backend = new PipelineBackend(
            $GLOBALS['DB_DSN'],
            $GLOBALS['DB_USER'],
            $GLOBALS['DB_PASSWD'],
            [],
            'test_jobs'
        );
        parent::setUp();
    }

    public function testGetDefaultTableName() : void
    {
        $backend = new PipelineBackend($GLOBALS['DB_DSN'], $GLOBALS['DB_USER'], $GLOBALS['DB_PASSWD']);
        $this->assertEquals('emphloyer_jobs', $backend->getTableName());
    }

    public function testOverrideDefaultTableNameInConstructor() : void
    {
        $backend = new PipelineBackend(
            $GLOBALS['DB_DSN'],
            $GLOBALS['DB_USER'],
            $GLOBALS['DB_PASSWD'],
            [],
            'override_jobs'
        );
        $this->assertEquals('override_jobs', $backend->getTableName());
    }

    public function testSetAndGetTableName() : void
    {
        $backend = new PipelineBackend($GLOBALS['DB_DSN'], $GLOBALS['DB_USER'], $GLOBALS['DB_PASSWD']);
        $backend->setTableName('dummy_jobs');
        $this->assertEquals('dummy_jobs', $backend->getTableName());
    }
}
