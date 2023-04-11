<?php

namespace UltraCod3\PhpRdKafka;

class Topic {

    private string $name;

    public function __construct(string $name){
        $this->name = $name;
    }

    public function getName(): string {
        return $this->name;
    }

}