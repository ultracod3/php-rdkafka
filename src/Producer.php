<?php

namespace UltraCod3\PhpRdKafka;

use RdKafka\Producer as VendorProducer;
use RuntimeException;

class Producer {

    private array $configuration;
    private Topic $topic;
    private VendorProducer $producer;

    public function __construct(array $configuration, Topic $topic){
        $this->configuration = $configuration;
        $this->topic = $topic;

        $conf = new \RdKafka\Conf();

        foreach($this->configuration['producer'] as $key => $value){
            $conf->set($key, $value);
        }

        foreach($this->configuration['global'] as $key => $value){
            if (!isset($this->configuration['producer'][$key])){
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
        }

        if (isset($this->configuration['stats_cb'])) {
            $conf->setStatsCb($this->configuration['stats_cb']);
        }

        if (isset($this->configuration['log_cb'])) {
            $conf->setLogCb($this->configuration['log_cb']);
        }

        $this->producer = new VendorProducer($conf);

        register_shutdown_function([$this->producer, 'flush'], $this->configuration['shutdown_timeout'] ?? 5000);
    }

    public function send(Message $message, bool $flush_after_produce = false): void {
        $topic = $this->producer->newTopic($this->topic->getName());

        $topic->producev(RD_KAFKA_PARTITION_UA, 0 , $message->getBody(), $message->getKey(), $message->getHeaders());

        $this->producer->poll(0);

        if ($flush_after_produce){
            $this->flush();
        }
    }

    public function flush(bool $once = false): true {
        for ($i = 0; $i < ($once ? 1 : 5); $i++){
            $result = $this->producer->flush(5 * 1000);

            if ($result === RD_KAFKA_RESP_ERR_NO_ERROR) {
                return true;
            }
        }

        throw new RuntimeException('Unable to flush messages to kafka producer');
    }

}