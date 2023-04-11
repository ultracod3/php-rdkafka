<?php

namespace UltraCod3\PhpRdKafka;

class Context {

    private array $producers = [];
    private array $consumers = [];
    private array $configuration;

    public function __construct(array $configuration){
        $this->configuration = $configuration;
    }

    public function createProducer(Topic $topic): Producer {
        $topicName = $topic->getName();

        if (!isset($this->producers[$topicName])){
            $this->producers[$topicName] = new Producer($this->configuration, $topic);
        }

        return $this->producers[$topicName];
    }

    public function createConsumer(Topic $topic): Consumer {
        $topicName = $topic->getName();

        if (!isset($this->consumers[$topicName])){
            $this->consumers[$topicName] = new Consumer($this->configuration, $topic, $this);
        }

        return $this->consumers[$topicName];
    }

    public function createTopic(string $topic): Topic {
        return new Topic($topic);
    }

    public function createMessage(string $body): Message {
        return new Message($body);
    }

    public function close(): void {
        foreach($this->consumers as $consumer){
            $consumer->unsubscribe();
        }

        if (isset($this->producer)){
            $this->producer->flush();
        }
    }

}