<?php

namespace Emphloyer\Pdo;

class ScheduleEntryIterator extends \IteratorIterator {
  /**
   * @param \Traversable $source
   */
  public function __construct(\Traversable $source) {
    parent::__construct($source);
  }

  /**
   * Convert an array with schedule entry attributes into an object
   * @return \Emphloyer\Scheduler\ScheduleEntry
   */
  public function current() {
    $record = parent::current();
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
