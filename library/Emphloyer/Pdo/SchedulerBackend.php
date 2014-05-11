<?php

namespace Emphloyer\Pdo;

use \PDO;

/**
 * Pipeline Backend using PDO.
 */
class SchedulerBackend implements \Emphloyer\Scheduler\Backend {
  protected $pdoDsn;
  protected $pdoUser;
  protected $pdoPassword;
  protected $pdoAttributes;
  protected $pdo;

  /**
   * Instantiate a new PipelineBackend.
   * @param string $dsn PDO DSN string
   * @param string $user Database user
   * @param string $password Database password
   * @param array $attributes PDO driver attributes
   * @return PipelineBackend
   */
  public function __construct($dsn, $user = null, $password = null, $attributes = array()) {
    $this->pdoDsn = $dsn;
    $this->pdoUser = $user;
    $this->pdoPassword = $password;
    $this->pdoAttributes = $attributes;
    $this->reconnect();
  }

  /**
   * Reconnect PDO.
   */
  public function reconnect() {
    $this->pdo = null;
    $this->pdo = new PDO($this->pdoDsn, $this->pdoUser, $this->pdoPassword, $this->pdoAttributes);
    $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
  }

  public function clear() {
    $stmt = $this->pdo->prepare('TRUNCATE emphloyer_scheduled_jobs');
    $stmt->execute();
  }

  public function schedule(array $job, $minute = null, $hour = null, $dayOfMonth = null, $month = null, $dayOfWeek = null) {
    $uuid = uuid_create();
    $className = $job['className'];
    unset($job['className']);
    $stmt = $this->pdo->prepare('INSERT INTO emphloyer_scheduled_jobs (uuid, created_at, class_name, attributes, minute, hour, monthday, month, weekday) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
    if ($stmt->execute(array($uuid, strftime('%F %T'), $className, base64_encode(serialize($job)), $minute, $hour, $dayOfMonth, $month, $dayOfWeek))) {
      return $this->find($uuid);
    }
  }

  public function getJobsFor(\DateTime $dateTime, $lock = true) {
    $jobs = array();

    $minute = $dateTime->format("i");
    $hour = $dateTime->format("H");
    $dayOfMonth = $dateTime->format("d");
    $month = $dateTime->format("m");
    $dayOfWeek = $dateTime->format("w");

    $lockUuid = uuid_create();
    $params = array(
      'lock_uuid' => $lockUuid, 
      'locked_at' => $dateTime->format("Y-m-d H:i") . ":00",
      'minute' => $minute,
      'hour' => $hour,
      'monthday' => $dayOfMonth,
      'month' => $month,
      'weekday' => $dayOfWeek,
    );

    $andSql = array();

    foreach (array('minute', 'hour', 'monthday', 'month', 'weekday') as $field) {
      $andSql[] = "({$field} IS NULL OR {$field} = :{$field})";
    }

    $andSql = "AND " . implode(" AND ", $andSql);

    if ($lock) {
      $updateStatement = $this->pdo->prepare("UPDATE emphloyer_scheduled_jobs SET lock_uuid = :lock_uuid, locked_at = :locked_at WHERE (locked_at IS NULL OR locked_at < :locked_at) {$andSql}");
      if ($updateStatement->execute($params)) {
        $selectStatement = $this->pdo->prepare("SELECT * FROM emphloyer_scheduled_jobs WHERE lock_uuid = :lock_uuid ORDER BY id ASC");

        if ($selectStatement->execute(array('lock_uuid' => $lockUuid))) {
          while ($row = $selectStatement->fetch(PDO::FETCH_ASSOC)) {
            $jobs[] = $this->load($row);
          }
        }
      }
    } else {
      $selectStatement = $this->pdo->prepare("SELECT * FROM emphloyer_scheduled_jobs WHERE 1 {$andSql} ORDER BY id ASC");

      $params = array(
        'minute' => $minute,
        'hour' => $hour,
        'monthday' => $dayOfMonth,
        'month' => $month,
        'weekday' => $dayOfWeek,
      );
      if ($selectStatement->execute($params)) {
        while ($row = $selectStatement->fetch(PDO::FETCH_ASSOC)) {
          $jobs[] = $this->load($row);
        }
      }
    }

    return $jobs;
  }

  /**
   * Find a specific job in the schedule using its id and return its attributes.
   * @param mixed $id
   * @return array|null
   */
  protected function find($id) {
    $stmt = $this->pdo->prepare('SELECT * FROM emphloyer_scheduled_jobs WHERE uuid = ?');
    if ($stmt->execute(array($id))) {
      return $this->load($stmt->fetch(PDO::FETCH_ASSOC));
    }
  }

  /**
   * Convert a database record into a set of attributes that can be used to load a Job object.
   * @param array $record
   * @return array
   */
  protected function load($record) {
    $attributes = unserialize(base64_decode($record['attributes']));
    $attributes['id'] = $record['uuid'];
    $attributes['className'] = $record['class_name'];
    return $attributes;
  }
}
