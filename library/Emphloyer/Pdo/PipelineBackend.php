<?php

namespace Emphloyer\Pdo;

use PDO;

/**
 * Pipeline Backend using PDO.
 */
class PipelineBackend implements \Emphloyer\Pipeline\Backend
{
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
    public function __construct(
        $dsn,
        $user = null,
        $password = null,
        $attributes = array(),
        $tableName = "emphloyer_jobs"
    ) {
        $this->pdoDsn = $dsn;
        $this->pdoUser = $user;
        $this->pdoPassword = $password;
        $this->pdoAttributes = $attributes;
        $this->tableName = $tableName;
        $this->reconnect();
    }

    /**
     * Reconnect PDO.
     */
    public function reconnect()
    {
        $this->pdo = null;
        $this->pdo = new PDO($this->pdoDsn, $this->pdoUser, $this->pdoPassword, $this->pdoAttributes);
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    /**
     * Get the database table name.
     * @return string
     */
    public function getTableName()
    {
        return $this->tableName;
    }

    /**
     * Set the database table name to use.
     * @param string $tableName
     * @return PipelineBackend
     */
    public function setTableName($tableName)
    {
        $this->tableName = $tableName;
    }

    /**
     * Push a job onto the pipeline.
     * @param array $attributes Job attributes to save (must include the class name as 'className'
     * @param \DateTime|null $notBefore Date and time after which this job may be run
     * @return array $attributes Updated job attributes, the Pipeline will instantiate a new job instance with these updated attributes (this can be useful to pass a job id or some other attribute of importance back to the caller of this method).
     */
    public function enqueue($attributes, \DateTime $notBefore = null)
    {
        $uuid = uuid_create();
        $className = $attributes['className'];
        $type = $attributes['type'];
        unset($attributes['className']);
        unset($attributes['type']);
        $notBeforeStamp = null;
        if (!is_null($notBefore)) {
            $notBeforeStamp = $notBefore->format("Y-m-d H:i:s");
        }
        $stmt = $this->pdo->prepare('INSERT INTO ' . $this->tableName . ' (uuid, created_at, run_from, status, class_name, type, attributes) VALUES (?, ?, ?, ?, ?, ?, ?)');
        if ($stmt->execute(array(
            $uuid,
            strftime('%F %T'),
            $notBeforeStamp,
            'free',
            $className,
            $type,
            base64_encode(serialize($attributes))
        ))) {
            return $this->find($uuid);
        }
    }

    /**
     * Find a specific job in the pipeline using its id and return its attributes.
     * @param mixed $id
     * @return array|null
     */
    public function find($id)
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ' . $this->tableName . ' WHERE uuid = ?');
        if ($stmt->execute(array($id))) {
            return $this->load($stmt->fetch(PDO::FETCH_ASSOC));
        }
    }

    /**
     * Convert a database record into a set of attributes that can be used to load a Job object.
     * @param array $record
     * @return array
     */
    protected function load($record)
    {
        $attributes = unserialize(base64_decode($record['attributes']));
        $attributes['id'] = $record['uuid'];
        $attributes['status'] = $record['status'];
        $attributes['className'] = $record['class_name'];
        $attributes['type'] = $record['type'];
        return $attributes;
    }

    /**
     * Get a job from the pipeline and return its attributes.
     * @param array $options
     * @return array|null
     */
    public function dequeue(array $options = array())
    {
        $lock = uuid_create();
        $params = array('lock_uuid' => $lock, 'locked_at' => strftime('%F %T'));

        if (isset($options["only"])) {
            $andSql = array();
            foreach ($options["only"] as $idx => $type) {
                $params["type" . $idx] = $type;
                $andSql[] = ":type" . $idx;
            }
            $andSql = "AND type IN (" . implode(",", $andSql) . ")";
        } elseif (isset($options["exclude"])) {
            $andSql = array();
            foreach ($options["exclude"] as $idx => $type) {
                $params["type" . $idx] = $type;
                $andSql[] = ":type" . $idx;
            }
            $andSql = "AND type NOT IN (" . implode(",", $andSql) . ")";
        } else {
            $andSql = "";
        }

        $updateStatement = $this->pdo->prepare("UPDATE " . $this->tableName . " SET lock_uuid = :lock_uuid, status = 'locked', locked_at = :locked_at WHERE status = 'free' AND (run_from IS NULL OR run_from <= NOW()) {$andSql} ORDER BY created_at ASC LIMIT 1");
        if ($updateStatement->execute($params)) {
            $selectStatement = $this->pdo->prepare("SELECT * FROM " . $this->tableName . " WHERE status = 'locked' AND lock_uuid = :lock_uuid");
            if ($selectStatement->execute(array('lock_uuid' => $lock))) {
                return $this->load($selectStatement->fetch(PDO::FETCH_ASSOC));
            }
        }
    }

    /**
     * Delete all the jobs from the pipeline.
     */
    public function clear()
    {
        $stmt = $this->pdo->prepare('TRUNCATE ' . $this->tableName);
        $stmt->execute();
    }

    /**
     * Mark a job as completed.
     * @param array $attributes
     */
    public function complete($attributes)
    {
        if (isset($attributes['id'])) {
            $stmt = $this->pdo->prepare('DELETE FROM ' . $this->tableName . ' WHERE uuid = ?');
            $stmt->execute(array($attributes['id']));
        }
    }

    /**
     * Reset a job so it can be picked up again.
     * @param array $attributes
     */
    public function reset($attributes)
    {
        if (isset($attributes['id'])) {
            $id = $attributes['id'];
            $type = $attributes['type'];
            unset($attributes['type']);
            unset($attributes['className']);
            unset($attributes['id']);
            $stmt = $this->pdo->prepare("UPDATE " . $this->tableName . " SET status = 'free', lock_uuid = NULL, locked_at = NULL, type = ?, attributes = ? WHERE uuid = ?");
            $stmt->execute(array($type, base64_encode(serialize($attributes)), $id));
        }
    }

    /**
     * Mark a job as failed.
     * @param array $attributes
     */
    public function fail($attributes)
    {
        if (isset($attributes['id'])) {
            $id = $attributes['id'];
            $type = $attributes['type'];
            unset($attributes['type']);
            unset($attributes['className']);
            unset($attributes['id']);
            $stmt = $this->pdo->prepare("UPDATE " . $this->tableName . " SET status = 'failed', type = ?, attributes = ? WHERE uuid = ?");
            $stmt->execute(array($type, base64_encode(serialize($attributes)), $id));
        }
    }
}
