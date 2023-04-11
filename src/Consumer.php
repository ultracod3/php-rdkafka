<?php

namespace UltraCod3\PhpRdKafka;

use RdKafka\KafkaConsumer as VendorConsumer;
use RdKafka\Message as VendorMessage;
use RuntimeException;

class Consumer {

    private array $configuration;
    private Context $context;
    private Topic $topic;
    private bool $subscribed = false;
    private bool $commit_async;
    private VendorConsumer $consumer;

    public function __construct(array $configuration, Topic $topic, Context $context){
        $this->configuration = $configuration;
        $this->topic = $topic;
        $this->context = $context;

        $conf = new \RdKafka\Conf();

        $this->commit_async = $this->configuration['commit_async'] ?? true;

        foreach($this->configuration['consumer'] as $key => $value){
            $conf->set($key, $value);
        }

        foreach($this->configuration['global'] as $key => $value){
            if (!isset($this->configuration['consumer'][$key])){
                $conf->set($key, $value);
            }
        }

        if (isset($this->configuration['dr_msg_cb'])) {
            $conf->setDrMsgCb($this->configuration['dr_msg_cb']);
        }

        if (isset($this->configuration['error_cb'])) {
            $conf->setErrorCb($this->configuration['error_cb']);
        }

        if (isset($this->configuration['rebalance_cb'])) {
            $conf->setRebalanceCb($this->configuration['rebalance_cb']);
        }else{
            $conf->setRebalanceCb(function (VendorConsumer $kafka, $err, array $partitions = null): void {
                switch ($err) {
                    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                        $kafka->assign($partitions);
                    break;
                    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                        $kafka->assign(null);
                    break;
                    default:
                        throw new RuntimeException($err);
                }
            });
        }

        if (isset($this->configuration['stats_cb'])) {
            $conf->setStatsCb($this->configuration['stats_cb']);
        }

        if (isset($this->configuration['log_cb'])) {
            $conf->setLogCb($this->configuration['log_cb']);
        }

        $this->consumer = new VendorConsumer($conf);
    }

    public function acknowledge(Message $message): void {
        if ($this->commit_async) {
            $this->consumer->commitAsync($message->getKafkaMessage());
        } else {
            $this->consumer->commit($message->getKafkaMessage());
        }
    }

    public function reject(Message $message, bool $requeue = false): void {
        $this->acknowledge($message);

        if ($requeue){
            $this->context->createProducer($this->topic)->send($message);
        }
    }

    public function unsubscribe(): void {
        $this->consumer->unsubscribe();
    }

    public function receive(int $timeout = 0): ?Message {
        if ($this->subscribed === false){
            $this->consumer->subscribe([$this->topic->getName()]);

            register_shutdown_function([$this->consumer, 'unsubscribe']);

            $this->subscribed = true;
        }

        if ($timeout > 0) {
            return $this->doReceive($timeout);
        }

        while (true) {
            if ($message = $this->doReceive(500)) {
                return $message;
            }
        }

        return null;
    }

    private function doReceive(int $timeout): ?Message {
        /** @var VendorMessage|null $kafkaMessage */
        $kafkaMessage = $this->consumer->consume($timeout);

        switch ($kafkaMessage->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $message = new Message;
                $message->setBody($kafkaMessage->payload);
                $message->setKey($kafkaMessage->key);
                $message->setHeaders($kafkaMessage->headers);
                $message->setKafkaMessage($kafkaMessage);

                return $message;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                return null;
            default:
                throw new RuntimeException($kafkaMessage->errstr(), $kafkaMessage->err);
        }
    }

}