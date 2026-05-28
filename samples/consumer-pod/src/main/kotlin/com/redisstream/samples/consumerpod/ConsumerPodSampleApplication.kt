package com.redisstream.samples.consumerpod

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ConsumerPodSampleApplication

fun main(args: Array<String>) {
    runApplication<ConsumerPodSampleApplication>(*args)
}
