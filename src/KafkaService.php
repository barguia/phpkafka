<?php

namespace Barguia\PhpRdkafka;

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
    private Message $message;

    /**
     * @param String $groupId
     * @param String $topic
     * @param ConsumerFunction $parse
     */
    public function __construct(String $groupId, String $topic, ConsumerFunction $parse, array $config)
    {
        $this->parse = $parse;
        $this->groupId = $groupId;
        $this->topic = $topic;

        $this->configuracao =array_merge($config, [
            'auto.commit.interval.ms' => 0,
            'group.id' => $this->groupId,
        ]);

        $this->config = new ConfiguracaoDefault($this->configuracao);

        $this->setConsumer();
    }

    public function commit(): void
    {
        $this->consumer->commit($this->message);
    }

    private function configuracao(): Conf
    {
        $conf = new Conf();
        $conf->setRebalanceCb(function (KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    echo "Assign partitions".PHP_EOL;
                    $kafka->assign($partitions);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    echo "Revoke partitions".PHP_EOL;
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
        $this->message = $this->consumer->consume(120 * 1000);
        switch ($this->message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $this->parse->consume(unserialize($this->message->payload), $this->message->headers);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                /**
                 * nao há mensagens na particacao do Topico
                 * Aguardando novas mensagens
                 */
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                #echo "Timed out".PHP_EOL;
                break;
            default:
                throw new \Exception($this->message->errstr(), $this->message->err);
                break;
        }
        return $this->message;
    }

    public function run(): void
    {
        echo "Inicio: ".date('Y-m-d H:i:s').PHP_EOL;
        echo "Consummer: {$this->groupId} ".PHP_EOL;
        echo "Topic: ".$this->topic.PHP_EOL.PHP_EOL;

        while (true) {
            $this->consumeUmaMenagem();
        }
    }

    public function getMessage(): Message
    {
        return $this->message;
    }

    private function setConsumer(): void
    {
        $this->consumer = new KafkaConsumer($this->configuracao());
        $this->consumer->subscribe([$this->topic]);
    }
}
