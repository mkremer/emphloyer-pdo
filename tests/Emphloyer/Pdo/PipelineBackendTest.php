<?php

namespace Emphloyer\Pdo;

class PipelineBackendTestJob extends \Emphloyer\AbstractJob {
  public function setName($name) {
    $this->attributes['name'] = $name;
  }

  public function getName() {
    return $this->attributes['name'];
  }

  public function getStatus() {
    return $this->attributes['status'];
  }

  public function perform() {
  }
}

class PipelineBackendTest extends \PHPUnit_Framework_TestCase {
  public function setUp() {
    $this->pdo = new \PDO($GLOBALS['DB_DSN'], $GLOBALS['DB_USER'], $GLOBALS['DB_PASSWD']);
    $this->pdo->exec('DROP TABLE IF EXISTS emphloyer_jobs');
    $this->pdo->exec('CREATE table emphloyer_jobs (uuid VARCHAR(36) PRIMARY KEY, created_at TIMESTAMP, locked_at TIMESTAMP, lock_uuid VARCHAR(36) UNIQUE, status VARCHAR(20), class_name VARCHAR(255), attributes TEXT);');
    $this->backend = new PipelineBackend($GLOBALS['DB_DSN'], $GLOBALS['DB_USER'], $GLOBALS['DB_PASSWD']);
    $this->pipeline = new \Emphloyer\Pipeline($this->backend);
  }

  public function testEnqueue() {
    $job = new PipelineBackendTestJob();
    $job->setName('Job 1');

    $queuedJob = $this->pipeline->enqueue($job);
    $jobId1 = $queuedJob->getId();
    $this->assertNotNull($jobId1);
    $this->assertEquals('Job 1', $queuedJob->getName());
    $this->assertEquals('free', $queuedJob->getStatus());

    $job = new PipelineBackendTestJob();
    $job->setName('Job 2');

    $queuedJob = $this->pipeline->enqueue($job);
    $jobId2 = $queuedJob->getId();
    $this->assertNotNull($jobId2);
    $this->assertNotEquals($jobId1, $jobId2);
    $this->assertEquals('Job 2', $queuedJob->getName());
    $this->assertEquals('free', $queuedJob->getStatus());
  }

  public function testDequeue() {
    $job = new PipelineBackendTestJob();
    $job->setName('Job 1');
    $job1 = $this->pipeline->enqueue($job);
    sleep(1);
    
    $job = new PipelineBackendTestJob();
    $job->setName('Job 2');
    $job2 = $this->pipeline->enqueue($job);

    $job = $this->pipeline->dequeue();
    $this->assertEquals($job1->getId(), $job->getId());
    $this->assertEquals('Job 1', $job->getName());
    $this->assertEquals('locked', $job->getStatus());

    $job = $this->pipeline->dequeue();
    $this->assertEquals($job2->getId(), $job->getId());
    $this->assertEquals('Job 2', $job->getName());
    $this->assertEquals('locked', $job->getStatus());

    $this->assertNull($this->pipeline->dequeue());
  }

  public function testFindJob() {
    $job1 = new PipelineBackendTestJob();
    $job1->setName('Job 1');

    $job2 = new PipelineBackendTestJob();
    $job2->setName('Job 2');

    $job3 = new PipelineBackendTestJob();
    $job3->setName('Job 3');

    $job1 = $this->pipeline->enqueue($job1);
    sleep(1);
    $job2 = $this->pipeline->enqueue($job2);
    sleep(1);
    $job3 = $this->pipeline->enqueue($job3);

    $lockedJob = $this->pipeline->dequeue();
    $failedJob = $this->pipeline->dequeue();
    $this->pipeline->fail($failedJob);

    $foundJob = $this->pipeline->find($job1->getId());
    $this->assertEquals($job1->getId(), $foundJob->getId());
    $this->assertEquals('Job 1', $foundJob->getName());
    $this->assertEquals('locked', $foundJob->getStatus());

    $foundJob = $this->pipeline->find($job2->getId());
    $this->assertEquals($job2->getId(), $foundJob->getId());
    $this->assertEquals('Job 2', $foundJob->getName());
    $this->assertEquals('failed', $foundJob->getStatus());

    $foundJob = $this->pipeline->find($job3->getId());
    $this->assertEquals($job3->getId(), $foundJob->getId());
    $this->assertEquals('Job 3', $foundJob->getName());
    $this->assertEquals('free', $foundJob->getStatus());
  }

  public function testClear() {
    $job1 = new PipelineBackendTestJob();
    $job1->setName('Job 1');

    $job2 = new PipelineBackendTestJob();
    $job2->setName('Job 2');

    $job3 = new PipelineBackendTestJob();
    $job3->setName('Job 3');

    $job1 = $this->pipeline->enqueue($job1);
    $job2 = $this->pipeline->enqueue($job2);
    $job3 = $this->pipeline->enqueue($job3);

    $lockedJob = $this->pipeline->dequeue();
    $failedJob = $this->pipeline->dequeue();
    $this->pipeline->fail($failedJob);

    $this->pipeline->clear();
    $this->assertNull($this->pipeline->find($job1->getId()));
    $this->assertNull($this->pipeline->find($job2->getId()));
    $this->assertNull($this->pipeline->find($job3->getId()));
  }

  public function testComplete() {
    $job = new PipelineBackendTestJob();
    $job->setName('Job 1');
    $savedJob = $this->pipeline->enqueue($job);
    $this->assertNotNull($savedJob->getId());

    $job = $this->pipeline->dequeue();
    $this->assertEquals($savedJob->getId(), $job->getId());
    $this->assertNotNull($this->pipeline->find($savedJob->getId()));

    $this->pipeline->complete($job);
    $this->assertNull($this->pipeline->find($job->getId()));
  }

  public function testFail() {
    $job = new PipelineBackendTestJob();
    $job->setName('Job 1');
    $savedJob = $this->pipeline->enqueue($job);
    $this->assertNotNull($savedJob->getId());

    $job = $this->pipeline->dequeue();
    $this->assertEquals($savedJob->getId(), $job->getId());
    $this->assertNotNull($this->pipeline->find($savedJob->getId()));

    $job->setName('Failed Job 1');
    $this->pipeline->fail($job);
    $job = $this->pipeline->find($savedJob->getId());
    $this->assertNotNull($job);
    $this->assertEquals('failed', $job->getStatus());
    $this->assertEquals('Failed Job 1', $job->getName());
    $this->assertNull($this->pipeline->dequeue());
  }

  public function testResetFailedJob() {
    $job = new PipelineBackendTestJob();
    $job->setName('Job 1');
    $savedJob = $this->pipeline->enqueue($job);
    $this->assertNotNull($savedJob->getId());

    $job = $this->pipeline->dequeue();
    $this->assertEquals($savedJob->getId(), $job->getId());
    $this->assertNotNull($this->pipeline->find($savedJob->getId()));

    $this->pipeline->fail($job);
    $job = $this->pipeline->find($job->getId());
    $this->assertEquals('failed', $job->getStatus());
    $job->setName('Reset Job 1');
    $this->pipeline->reset($job);
    $job = $this->pipeline->find($job->getId());
    $this->assertEquals('free', $job->getStatus());
    $this->assertEquals('Reset Job 1', $job->getName());

    $job = $this->pipeline->dequeue();
    $this->assertEquals($savedJob->getId(), $job->getId());
    $this->assertNotNull($this->pipeline->find($savedJob->getId()));
  }

  public function testResetLockedJob() {
    $job = new PipelineBackendTestJob();
    $job->setName('Job 1');
    $savedJob = $this->pipeline->enqueue($job);
    $this->assertNotNull($savedJob->getId());

    $job = $this->pipeline->dequeue();
    $this->assertEquals($savedJob->getId(), $job->getId());
    $this->assertNotNull($this->pipeline->find($savedJob->getId()));
    $this->assertEquals('locked', $this->pipeline->find($savedJob->getId())->getStatus());

    $this->assertNull($this->pipeline->dequeue());
    $this->pipeline->reset($job);
    $this->assertNotNull($this->pipeline->find($savedJob->getId()));
    $this->assertEquals('free', $this->pipeline->find($savedJob->getId())->getStatus());

    $job = $this->pipeline->dequeue();
    $this->assertEquals($savedJob->getId(), $job->getId());
    $this->assertNotNull($this->pipeline->find($savedJob->getId()));
    $this->assertEquals('locked', $this->pipeline->find($savedJob->getId())->getStatus());
  }
}
