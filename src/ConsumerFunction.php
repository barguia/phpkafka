<?php

namespace Barguia\PhpRdkafka;

use RdKafka\Message;

interface ConsumerFunction
{
    public function consume(string $message, array $headers);
}
