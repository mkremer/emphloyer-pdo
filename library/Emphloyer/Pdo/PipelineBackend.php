<?php

namespace Emphloyer\Pdo;

use \PDO;

/**
 * Pipeline Backend using PDO.
 */
class PipelineBackend implements \Emphloyer\Pipeline\Backend {
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

  /**
   * Push a job onto the pipeline.
   * @param array $attributes Job attributes to save (must include the class name as 'className'
   * @return array $attributes Updated job attributes, the Pipeline will instantiate a new job instance with these updated attributes (this can be useful to pass a job id or some other attribute of importance back to the caller of this method).
   */
  public function enqueue($attributes) {
    $uuid = uuid_create();
    $className = $attributes['className'];
    unset($attributes['className']);
    $stmt = $this->pdo->prepare('INSERT INTO emphloyer_jobs (uuid, created_at, status, class_name, attributes) VALUES (?, ?, ?, ?, ?)');
    if ($stmt->execute(array($uuid, strftime('%F %T'), 'free', $className, base64_encode(serialize($attributes))))) {
      return $this->find($uuid);
    }
  }

  /**
   * Get a job from the pipeline and return its attributes.
   * @return array|null
   */
  public function dequeue() {
    $lock = uuid_create();
    $updateStatement = $this->pdo->prepare("UPDATE emphloyer_jobs SET lock_uuid = ?, status = 'locked', locked_at = ? WHERE status = 'free' ORDER BY created_at ASC LIMIT 1");
    if ($updateStatement->execute(array($lock, strftime('%F %T')))) {
      $selectStatement = $this->pdo->prepare("SELECT * FROM emphloyer_jobs WHERE status = 'locked' AND lock_uuid = ?");
      if ($selectStatement->execute(array($lock))) {
        return $this->load($selectStatement->fetch(PDO::FETCH_ASSOC));
      }
    }
  }

  /**
   * Find a specific job in the pipeline using its id and return its attributes.
   * @param mixed $id
   * @return array|null
   */
  public function find($id) {
    $stmt = $this->pdo->prepare('SELECT * FROM emphloyer_jobs WHERE uuid = ?');
    if ($stmt->execute(array($id))) {
      return $this->load($stmt->fetch(PDO::FETCH_ASSOC));
    }
  }

  /**
   * Delete all the jobs from the pipeline.
   */
  public function clear() {
    $stmt = $this->pdo->prepare('TRUNCATE emphloyer_jobs');
    $stmt->execute();
  }

  /**
   * Mark a job as completed.
   * @param array $attributes
   */
  public function complete($attributes) {
    if (isset($attributes['id'])) {
      $stmt = $this->pdo->prepare('DELETE FROM emphloyer_jobs WHERE uuid = ?');
      $stmt->execute(array($attributes['id']));
    }
  }

  /**
   * Reset a job so it can be picked up again.
   * @param array $attributes
   */
  public function reset($attributes) {
    if (isset($attributes['id'])) {
      $stmt = $this->pdo->prepare("UPDATE emphloyer_jobs SET status = 'free', lock_uuid = NULL WHERE uuid = ?");
      $stmt->execute(array($attributes['id']));
    }
  }

  /**
   * Mark a job as failed.
   * @param array $attributes
   */
  public function fail($attributes) {
    if (isset($attributes['id'])) {
      $stmt = $this->pdo->prepare("UPDATE emphloyer_jobs SET status = 'failed' WHERE uuid = ?");
      $stmt->execute(array($attributes['id']));
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
    $attributes['status'] = $record['status'];
    $attributes['className'] = $record['class_name'];
    return $attributes;
  }
}
