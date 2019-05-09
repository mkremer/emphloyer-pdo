<?php

declare(strict_types=1);

namespace Emphloyer\Pdo;

use IteratorIterator;
use function base64_decode;
use function unserialize;

class ScheduleEntryIterator extends IteratorIterator
{
    /**
     * Convert an array with schedule entry attributes into an object
     *
     * @return mixed[]
     */
    public function current() : array
    {
        $record                     = parent::current();
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
}
