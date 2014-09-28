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
  protected $tableName;

  /**
   * Instantiate a new PipelineBackend.
   * @param string $dsn PDO DSN string
   * @param string $user Database user
   * @param string $password Database password
   * @param array $attributes PDO driver attributes
   * @param string $tableName Database table to use
   * @return PipelineBackend
   */
  public function __construct($dsn, $user = null, $password = null, $attributes = array(), $tableName = "emphloyer_scheduled_jobs") {
    $this->pdoDsn = $dsn;
    $this->pdoUser = $user;
    $this->pdoPassword = $password;
    $this->pdoAttributes = $attributes;
    $this->reconnect();
    $this->tableName = $tableName;
  }

  /**
   * Get the database table name.
   * @return string
   */
  public function getTableName() {
    return $this->tableName;
  }

  /**
   * Set the database table name to use.
   * @param string $tableName
   * @return PipelineBackend
   */
  public function setTableName($tableName) {
    $this->tableName = $tableName;
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
    $stmt = $this->pdo->prepare('TRUNCATE ' . $this->tableName);
    $stmt->execute();
  }

  public function schedule(array $job, $minute = null, $hour = null, $dayOfMonth = null, $month = null, $dayOfWeek = null) {
    $uuid = uuid_create();
    $className = $job['className'];
    unset($job['className']);
    $stmt = $this->pdo->prepare('INSERT INTO ' . $this->tableName . ' (uuid, created_at, class_name, attributes, minute, hour, monthday, month, weekday) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
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
      $updateStatement = $this->pdo->prepare("UPDATE " . $this->tableName . " SET lock_uuid = :lock_uuid, locked_at = :locked_at WHERE (locked_at IS NULL OR locked_at < :locked_at) {$andSql}");
      if ($updateStatement->execute($params)) {
        $selectStatement = $this->pdo->prepare("SELECT * FROM " . $this->tableName . " WHERE lock_uuid = :lock_uuid ORDER BY id ASC");

        if ($selectStatement->execute(array('lock_uuid' => $lockUuid))) {
          while ($row = $selectStatement->fetch(PDO::FETCH_ASSOC)) {
            $entry = $this->load($row);
            $jobs[] = $entry["job"];
          }
        }
      }
    } else {
      $selectStatement = $this->pdo->prepare("SELECT * FROM " . $this->tableName . " WHERE 1 {$andSql} ORDER BY id ASC");

      $params = array(
        'minute' => $minute,
        'hour' => $hour,
        'monthday' => $dayOfMonth,
        'month' => $month,
        'weekday' => $dayOfWeek,
      );
      if ($selectStatement->execute($params)) {
        while ($row = $selectStatement->fetch(PDO::FETCH_ASSOC)) {
          $entry = $this->load($row);
          $jobs[] = $entry["job"];
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
  public function find($id) {
    $stmt = $this->pdo->prepare('SELECT * FROM ' . $this->tableName . ' WHERE uuid = ?');
    if ($stmt->execute(array($id))) {
      if ($record = $stmt->fetch(PDO::FETCH_ASSOC)) {
        return $this->load($record);
      }
    }
  }

  /**
   * Delete a specific entry in the schedule using its id and return its attributes.
   * @param mixed $id
   * @return array|null
   */
  public function delete($id) {
    $stmt = $this->pdo->prepare('DELETE FROM ' . $this->tableName . ' WHERE uuid = ?');
    $stmt->execute(array($id));
  }

  /**
   * Get all entries in the schedule
   */
  public function allEntries() {
    $stmt = $this->pdo->prepare("SELECT * FROM " . $this->tableName . " ORDER BY id ASC;");
    $stmt->execute();
    return new ScheduleEntryIterator($stmt);
  }

  /**
   * Convert a database record into a set of attributes that can be used to load a Job object.
   * @param array $record
   * @return array
   */
  protected function load($record) {
    $jobAttributes = unserialize(base64_decode($record['attributes']));
    $jobAttributes['id'] = $record['uuid'];
    $jobAttributes['className'] = $record['class_name'];
    $attributes = array(
      'id' => $record['uuid'],
      'job' => $jobAttributes,
      'minute' => $record['minute'],
      'hour' => $record['hour'],
      'dayOfMonth' => $record['monthday'],
      'month' => $record['month'],
      'dayOfWeek' => $record['weekday'],
    );
    return $attributes;
  }
}
