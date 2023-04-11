<?php

namespace UltraCod3\PhpRdKafka;

class Factory {

    public static function createContext(array $configuration = []): Context {
        $config = array_merge([
            'global' => [
                'metadata.broker.list' => 'localhost:9092'
            ],
            'consumer' => [
                'consumer' => uniqid('', true)
            ],
            'producer' => []
        ], $configuration);

        return new Context($config);
    }

}