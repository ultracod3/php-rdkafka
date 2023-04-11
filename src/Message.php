<?php

namespace UltraCod3\PhpRdKafka;

use RdKafka\Message as VendorMessage;

class Message {

    private ?string $body = null;
    private ?string $key = null;
    private array $headers = [];
    private VendorMessage $kafkaMessage;

    public function __construct(?string $body = null){
        $this->setBody($body);
    }

    public function setBody(?string $body): static {
        $this->body = $body;

        return $this;
    }

    public function getBody(): ?string {
        return $this->body;
    }

    public function setKey(?string $key): static {
        $this->key = $key;

        return $this;
    }

    public function getKey(): ?string {
        return $this->key;
    }

    public function setHeaders(array $headers): static {
        $this->headers = $headers;

        return $this;
    }

    public function getHeaders(): array {
        return $this->headers;
    }

    public function getKafkaMessage(): VendorMessage {
        return $this->kafkaMessage;
    }

    public function setKafkaMessage(VendorMessage $message): static {
        $this->kafkaMessage = $message;

        return $this;
    }

}