<?php

declare(strict_types=1);

namespace Emphloyer\Pdo;

use DateTime;
use Emphloyer\Scheduler\Backend;
use Iterator;
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
class SchedulerBackend implements Backend
{
    /** @var string */
    protected $pdoDsn;
    /** @var string|null */
    protected $pdoUser;
    /** @var string|null */
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
        string $tableName = 'emphloyer_scheduled_jobs'
    ) {
        $this->pdoDsn        = $dsn;
        $this->pdoUser       = $user;
        $this->pdoPassword   = $password;
        $this->pdoAttributes = $attributes;
        $this->reconnect();
        $this->tableName = $tableName;
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
    public function setTableName(string $tableName) : SchedulerBackend
    {
        $this->tableName = $tableName;

        return $this;
    }

    /** @inheritDoc */
    public function clear() : void
    {
        $stmt = $this->pdo->prepare('TRUNCATE ' . $this->tableName);
        $stmt->execute();
    }

    /** @inheritDoc */
    public function schedule(
        array $job,
        ?int $minute = null,
        ?int $hour = null,
        ?int $dayOfMonth = null,
        ?int $month = null,
        ?int $dayOfWeek = null
    ) : array {
        $uuid      = Uuid::uuid4()->toString();
        $className = $job['className'];
        unset($job['className']);
        $stmt = $this->pdo->prepare(
            'INSERT INTO ' . $this->tableName . ' (uuid, created_at, class_name, attributes, `minute`, `hour`, monthday, `month`, weekday) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
        );
        if ($stmt->execute(
            [
                $uuid,
                strftime('%F %T'),
                $className,
                base64_encode(serialize($job)),
                $minute,
                $hour,
                $dayOfMonth,
                $month,
                $dayOfWeek,
            ]
        )) {
            return $this->find($uuid);
        }
    }

    /**
     * Find a specific job in the schedule using its id and return its attributes.
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
        if ($record) {
            return $this->load($record);
        }

        return null;
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
        $jobAttributes              = unserialize(base64_decode($record['attributes']));
        $jobAttributes['id']        = $record['uuid'];
        $jobAttributes['className'] = $record['class_name'];

        return [
            'id' => $record['uuid'],
            'job' => $jobAttributes,
            'minute' => $record['minute'] ? (int) $record['minute'] : null,
            'hour' => $record['hour'] ? (int) $record['hour'] : null,
            'dayOfMonth' => $record['monthday'] ? (int) $record['monthday'] : null,
            'month' => $record['month'] ? (int) $record['month'] : null,
            'dayOfWeek' => $record['weekday'] ? (int) $record['weekday'] : null,
        ];
    }

    /** @inheritDoc */
    public function getJobsFor(DateTime $dateTime, bool $lock = true) : array
    {
        $jobs = [];

        $minute     = $dateTime->format('i');
        $hour       = $dateTime->format('H');
        $dayOfMonth = $dateTime->format('d');
        $month      = $dateTime->format('m');
        $dayOfWeek  = $dateTime->format('w');

        $lockUuid = Uuid::uuid4()->toString();
        $params   = [
            'lock_uuid' => $lockUuid,
            'locked_at' => $dateTime->format('Y-m-d H:i') . ':00',
            'minute' => $minute,
            'hour' => $hour,
            'monthday' => $dayOfMonth,
            'month' => $month,
            'weekday' => $dayOfWeek,
        ];

        $andSql = [];

        foreach (['minute', 'hour', 'monthday', 'month', 'weekday'] as $field) {
            $andSql[] = "({$field} IS NULL OR {$field} = :{$field})";
        }

        $andSql = 'AND ' . implode(' AND ', $andSql);

        if ($lock) {
            $updateStatement = $this->pdo->prepare(
                'UPDATE ' . $this->tableName . " 
                          SET lock_uuid = :lock_uuid, locked_at = :locked_at 
                          WHERE (locked_at IS NULL OR locked_at < :locked_at) {$andSql}"
            );

            if ($updateStatement->execute($params)) {
                $selectStatement = $this->pdo->prepare(
                    'SELECT * FROM ' . $this->tableName . ' WHERE lock_uuid = :lock_uuid ORDER BY id ASC'
                );

                if ($selectStatement->execute(['lock_uuid' => $lockUuid])) {
                    while ($row = $selectStatement->fetch(PDO::FETCH_ASSOC)) {
                        $entry  = $this->load($row);
                        $jobs[] = $entry['job'];
                    }
                }
            }
        } else {
            $selectStatement = $this->pdo->prepare(
                'SELECT * FROM ' . $this->tableName . " WHERE 1 {$andSql} ORDER BY id ASC"
            );

            $params = [
                'minute' => $minute,
                'hour' => $hour,
                'monthday' => $dayOfMonth,
                'month' => $month,
                'weekday' => $dayOfWeek,
            ];
            if ($selectStatement->execute($params)) {
                while ($row = $selectStatement->fetch(PDO::FETCH_ASSOC)) {
                    $entry  = $this->load($row);
                    $jobs[] = $entry['job'];
                }
            }
        }

        return $jobs;
    }

    /** @inheritDoc */
    public function delete($id) : void
    {
        $stmt = $this->pdo->prepare('DELETE FROM ' . $this->tableName . ' WHERE uuid = ?');
        $stmt->execute([$id]);
    }

    public function allEntries() : Iterator
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ' . $this->tableName . ' ORDER BY id ASC;');
        $stmt->execute();

        return new ScheduleEntryIterator($stmt);
    }
}
