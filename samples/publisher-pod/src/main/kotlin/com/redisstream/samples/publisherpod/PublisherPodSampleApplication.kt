package com.redisstream.samples.publisherpod

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@EnableScheduling
@SpringBootApplication
class PublisherPodSampleApplication

fun main(args: Array<String>) {
    runApplication<PublisherPodSampleApplication>(*args)
}
