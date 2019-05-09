<?php

declare(strict_types=1);

namespace Emphloyer\Pdo;

use DateTime;
use Emphloyer\Pipeline\Backend;
use PDO;
use Ramsey\Uuid\Uuid;
use function base64_decode;
use function base64_encode;
use function implode;
use function serialize;
use function strftime;
use function unserialize;

/**
 * Pipeline Backend using PDO.
 */
class PipelineBackend implements Backend
{
    /** @var string */
    protected $pdoDsn;
    /** @var string */
    protected $pdoUser;
    /** @var string */
    protected $pdoPassword;
    /** @var mixed[] */
    protected $pdoAttributes;
    /** @var PDO */
    protected $pdo;
    /** @var string */
    protected $tableName;

    /**
     * Instantiate a new PipelineBackend.
     *
     * @param string  $dsn        PDO DSN string
     * @param string  $user       Database user
     * @param string  $password   Database password
     * @param mixed[] $attributes PDO driver attributes
     * @param string  $tableName  Database table to use
     */
    public function __construct(
        string $dsn,
        ?string $user = null,
        ?string $password = null,
        array $attributes = [],
        string $tableName = 'emphloyer_jobs'
    ) {
        $this->pdoDsn        = $dsn;
        $this->pdoUser       = $user;
        $this->pdoPassword   = $password;
        $this->pdoAttributes = $attributes;
        $this->tableName     = $tableName;
        $this->reconnect();
    }

    /**
     * Reconnect PDO.
     */
    public function reconnect() : void
    {
        $this->pdo = null;
        $this->pdo = new PDO($this->pdoDsn, $this->pdoUser, $this->pdoPassword, $this->pdoAttributes);
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    /**
     * Get the database table name.
     */
    public function getTableName() : string
    {
        return $this->tableName;
    }

    /**
     * Set the database table name to use.
     */
    public function setTableName(string $tableName) : PipelineBackend
    {
        $this->tableName = $tableName;

        return $this;
    }

    /**
     * Push a job onto the pipeline.
     *
     * @param mixed[]       $attributes Job attributes to save (must include the class name as 'className'
     * @param DateTime|null $notBefore  Date and time after which this job may be run
     *
     * @return mixed[] $attributes Updated job attributes, the Pipeline will instantiate a new job instance with these
     *                             updated attributes (this can be useful to pass a job id or some other attribute of
     *                             importance back to the caller of this method).
     */
    public function enqueue(array $attributes, ?DateTime $notBefore = null) : array
    {
        $uuid      = Uuid::uuid4()->toString();
        $className = $attributes['className'];
        $type      = $attributes['type'];
        unset($attributes['className']);
        unset($attributes['type']);
        $notBeforeStamp = null;
        if ($notBefore !== null) {
            $notBeforeStamp = $notBefore->format('Y-m-d H:i:s');
        }
        $stmt = $this->pdo->prepare(
            'INSERT INTO ' . $this->tableName . ' (uuid, created_at, run_from, status, class_name, type, attributes) VALUES (?, ?, ?, ?, ?, ?, ?)'
        );
        if ($stmt->execute(
            [
                $uuid,
                strftime('%F %T'),
                $notBeforeStamp,
                'free',
                $className,
                $type,
                base64_encode(serialize($attributes)),
            ]
        )) {
            return $this->find($uuid);
        }
    }

    /**
     * Find a specific job in the pipeline using its id and return its attributes.
     *
     * @param mixed $id
     *
     * @return mixed[]|null
     */
    public function find($id) : ?array
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ' . $this->tableName . ' WHERE uuid = ?');
        if (! $stmt->execute([$id])) {
            return null;
        }

        $record = $stmt->fetch(PDO::FETCH_ASSOC);

        return $record ? $this->load($record) : null;
    }

    /**
     * Convert a database record into a set of attributes that can be used to load a Job object.
     *
     * @param mixed[] $record
     *
     * @return mixed[]
     */
    protected function load(array $record) : array
    {
        $attributes              = unserialize(base64_decode($record['attributes']));
        $attributes['id']        = $record['uuid'];
        $attributes['status']    = $record['status'];
        $attributes['className'] = $record['class_name'];
        $attributes['type']      = $record['type'];

        return $attributes;
    }

    /**
     * Get a job from the pipeline and return its attributes.
     *
     * @param mixed[] $options
     *
     * @return mixed[]|null
     */
    public function dequeue(array $options = []) : ?array
    {
        $lock   = Uuid::uuid4()->toString();
        $params = ['lock_uuid' => $lock, 'locked_at' => strftime('%F %T')];

        if (isset($options['only'])) {
            $andSql = [];
            foreach ($options['only'] as $idx => $type) {
                $params['type' . $idx] = $type;
                $andSql[]              = ':type' . $idx;
            }
            $andSql = 'AND type IN (' . implode(',', $andSql) . ')';
        } elseif (isset($options['exclude'])) {
            $andSql = [];
            foreach ($options['exclude'] as $idx => $type) {
                $params['type' . $idx] = $type;
                $andSql[]              = ':type' . $idx;
            }
            $andSql = 'AND type NOT IN (' . implode(',', $andSql) . ')';
        } else {
            $andSql = '';
        }

        $updateStatement = $this->pdo->prepare(
            'UPDATE ' . $this->tableName . " SET lock_uuid = :lock_uuid, status = 'locked', locked_at = :locked_at WHERE status = 'free' AND (run_from IS NULL OR run_from <= NOW()) {$andSql} ORDER BY created_at ASC LIMIT 1"
        );
        if (! $updateStatement->execute($params)) {
            return null;
        }

        $selectStatement = $this->pdo->prepare(
            'SELECT * FROM ' . $this->tableName . " WHERE status = 'locked' AND lock_uuid = :lock_uuid"
        );
        if (! $selectStatement->execute(['lock_uuid' => $lock])) {
            return null;
        }

        $record = $selectStatement->fetch(PDO::FETCH_ASSOC);

        return $record ? $this->load($record) : null;
    }

    /**
     * Delete all the jobs from the pipeline.
     */
    public function clear() : void
    {
        $stmt = $this->pdo->prepare('TRUNCATE ' . $this->tableName);
        $stmt->execute();
    }

    /**
     * Mark a job as completed.
     *
     * @param mixed[] $attributes
     */
    public function complete(array $attributes) : void
    {
        if (! isset($attributes['id'])) {
            return;
        }

        $stmt = $this->pdo->prepare('DELETE FROM ' . $this->tableName . ' WHERE uuid = ?');
        $stmt->execute([$attributes['id']]);
    }

    /**
     * Reset a job so it can be picked up again.
     *
     * @param mixed[] $attributes
     */
    public function reset(array $attributes) : void
    {
        if (! isset($attributes['id'])) {
            return;
        }

        $id   = $attributes['id'];
        $type = $attributes['type'];
        unset($attributes['type']);
        unset($attributes['className']);
        unset($attributes['id']);
        $stmt = $this->pdo->prepare(
            'UPDATE ' . $this->tableName . " SET status = 'free', lock_uuid = NULL, locked_at = NULL, type = ?, attributes = ? WHERE uuid = ?"
        );
        $stmt->execute([$type, base64_encode(serialize($attributes)), $id]);
    }

    /**
     * Mark a job as failed.
     *
     * @param mixed[] $attributes
     */
    public function fail(array $attributes) : void
    {
        if (! isset($attributes['id'])) {
            return;
        }

        $id   = $attributes['id'];
        $type = $attributes['type'];
        unset($attributes['type']);
        unset($attributes['className']);
        unset($attributes['id']);
        $stmt = $this->pdo->prepare(
            'UPDATE ' . $this->tableName . " SET status = 'failed', type = ?, attributes = ? WHERE uuid = ?"
        );
        $stmt->execute([$type, base64_encode(serialize($attributes)), $id]);
    }
}
