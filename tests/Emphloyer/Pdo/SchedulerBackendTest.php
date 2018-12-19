<?php

namespace Emphloyer\Pdo;

class SchedulerBackendTest extends \Emphloyer\Scheduler\BackendTestCase
{
    public function setUp()
    {
        $this->pdo = new \PDO($GLOBALS['DB_DSN'], $GLOBALS['DB_USER'], $GLOBALS['DB_PASSWD']);
        $this->pdo->exec('DROP TABLE IF EXISTS test_scheduled_jobs');
        $this->pdo->exec('CREATE TABLE test_scheduled_jobs (id INT AUTO_INCREMENT, uuid VARCHAR(36) UNIQUE, created_at TIMESTAMP, locked_at TIMESTAMP NULL DEFAULT NULL, lock_uuid VARCHAR(36), class_name VARCHAR(255), attributes TEXT, minute TINYINT(1) DEFAULT NULL, hour TINYINT(1) DEFAULT NULL, monthday TINYINT(1) DEFAULT NULL, month TINYINT(1) DEFAULT NULL, weekday TINYINT(1) DEFAULT NULL, PRIMARY KEY (id));');
        $this->backend = new SchedulerBackend($GLOBALS['DB_DSN'], $GLOBALS['DB_USER'], $GLOBALS['DB_PASSWD']);
        parent::setUp();
    }

    public function testGetDefaultTableName()
    {
        $backend = new SchedulerBackend($GLOBALS['DB_DSN'], $GLOBALS['DB_USER'], $GLOBALS['DB_PASSWD']);
        $this->assertEquals("emphloyer_scheduled_jobs", $backend->getTableName());
    }

    public function testOverrideDefaultTableNameInConstructor()
    {
        $backend = new SchedulerBackend($GLOBALS['DB_DSN'], $GLOBALS['DB_USER'], $GLOBALS['DB_PASSWD'], array(),
            "override_scheduled_jobs");
        $this->assertEquals("override_scheduled_jobs", $backend->getTableName());
    }

    public function testSetAndGetTableName()
    {
        $backend = new SchedulerBackend($GLOBALS['DB_DSN'], $GLOBALS['DB_USER'], $GLOBALS['DB_PASSWD']);
        $backend->setTableName("dummy_scheduled_jobs");
        $this->assertEquals("dummy_scheduled_jobs", $backend->getTableName());
    }
}
