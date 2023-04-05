<?php

namespace Barguia\Php74Rdkafka;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Message;

class KafkaService
{
    private $parse;
    private String $groupId;
    private ConfiguracaoDefault $config;
    private array $configuracao;
    private $topic;
    private int $autoCommit;

    /**
     * @param String $groupId
     * @param String $topic
     * @param ConsumerFunction $parse
     */
    public function __construct(String $groupId, String $topic, ConsumerFunction $parse, array $config)
    {
        $this->parse = $parse;
        $this->groupId = $groupId;

        $this->configuracao =array_merge($config, [
            'auto.commit.interval.ms' => 0,
            'group.id' => $this->groupId,
        ]);

        $this->config = new ConfiguracaoDefault($this->configuracao);

        $this->setConsumer($topic);
    }

    public function commit($message): void
    {
        $this->consumer->commit($message);
    }

    private function configuracao(): Conf
    {
        $conf = new Conf();
        $conf->setRebalanceCb(function (KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    echo "Assign: ";
                    var_dump($partitions);
                    $kafka->assign($partitions);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    echo "Revoke: ";
                    var_dump($partitions);
                    $kafka->assign(null);
                    break;

                default:
                    throw new \Exception($err);
            }
        });


        $this->config->setConfiguracao($conf);

        return $conf;
    }

    public function consumeUmaMenagem(): Message
    {
        $message = $this->consumer->consume(120 * 1000);
        echo "Iniciado Worker - Aguardando por novas mensagens".PHP_EOL;
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $this->parse->consume($message);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                #echo "No more messages; will wait for more\n";
                /**
                 * não há mensagens pendentes no tópico
                 */
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                echo "Timed out\n";
                break;
            default:
                throw new \Exception($message->errstr(), $message->err);
                break;
        }
        return $message;
    }

    public function run(): void
    {
        while (true) {
            $this->consumeUmaMenagem();
        }
    }

    private function setConsumer($topic): void
    {
        $this->consumer = new KafkaConsumer($this->configuracao());
        $this->consumer->subscribe([$topic]);
    }
}
