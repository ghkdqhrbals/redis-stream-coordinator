package com.redisstream.samples.publisherpod

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PublisherPodProperties::class)
class PublisherPodPropertiesConfiguration
