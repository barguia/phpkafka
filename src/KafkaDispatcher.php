<?php

namespace Barguia\PhpRdkafka;

use RdKafka\Conf;
use RdKafka\Producer;

class KafkaDispatcher
{
    private Producer $producer;
    private ConfiguracaoDefault $config;

    public function __construct(array $config = array())
    {
        $this->config = new ConfiguracaoDefault($config);
        $this->producer = new Producer($this->configuracao());
    }

    private function configuracao(): Conf
    {
        $conf = new Conf();
        $conf->setErrorCb(function ($kafka, $err, $reason) {
            printf("Kafka error: %s (reason: %s)\n", rd_kafka_err2str($err), $reason);
        });
        $this->config->setConfiguracao($conf);

        return $conf;
    }

    public function send(string $topic, $value, string $key = '', ?array $headers = []): bool
    {
        $topico = $this->producer->newTopic($topic);

        $topico->producev(
            RD_KAFKA_PARTITION_UA,
            0,
            serialize($value),
            $key,
            $headers
        );

        $this->producer->poll(0);

        for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
            $result = $this->producer->flush(10000);
            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new \RuntimeException('Was unable to flush, messages might be lost!');
        }
        return true;
    }

    /*
    public function sendBatch(array $messages)
    {
        $this->producer->sendBatch($messages);
    }
    */
}
