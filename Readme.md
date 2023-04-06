
# PHP Kafka client - php-rdkafka

Referência:
https://github.com/arnaud-lb/php-rdkafka



# PHP Kafka client - php-rdkafka

Referência:
https://github.com/arnaud-lb/php-rdkafka



## Producer/Examples

```php
<?php

use Barguia\PhpRdkafka\KafkaDispatcher;

require "vendor/autoload.php";

$config = array(
    'metadata.broker.list' => 'localhost:9092',
);
$producer = new KafkaDispatcher($config);
$headers = ['index1' => 'value1', 'index2' => 123];
$producer->send('topic_name', 'message', 'key', $headers);
```


## Consumer/Examples

```php
<?php

require "vendor/autoload.php";

use Barguia\PhpRdkafka\ConsumerFunction;
use Barguia\PhpRdkafka\KafkaService;


class ClassConsumer implements ConsumerFunction
{
    private KafkaService $consumer;
    public function __construct()
    {
        $config = array(
            'metadata.broker.list' => 'localhost:9092',
        );

        $this->consumer = new KafkaService('CONSUMER_GROUP_SGP_PHP_UNIT', 'SGP_PHP_UNIT', $this, $config);
        # Inicia o consumo do Topic
        $this->consume->run();
    }

    /**
    * @param string $message
    * @param array $headers
    * @return void  
    */
    
    public function consume(string $message, array $headers)
    {
        /**
        * Consome mensagens. O commit é opcional, depende da configuracao 
        */
        echo date('Y-m-d H:i:s ').$message.PHP_EOL;
        var_dump($headers);
        $this->consumer->commit();
    }
}

$consumer = new ClassConsumer();
```