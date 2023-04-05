<?php

namespace Barguia\Php74Rdkafka;

use RdKafka\Message;

interface ConsumerFunction
{
    public function consume(Message $message);
}
