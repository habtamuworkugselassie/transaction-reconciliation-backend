package com.reconciliation.config;

import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.infrastructure.messaging.ImportRequestedEvent;
import com.reconciliation.infrastructure.messaging.KafkaHealthIndicator;
import com.reconciliation.infrastructure.messaging.KafkaImportJobQueue;
import com.reconciliation.infrastructure.persistence.JooqImportJobStore;
import com.reconciliation.infrastructure.worker.ImportWorker;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jooq.DSLContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka wiring. The producer is idempotent with acks=all so an accepted event is
 * durable on the broker; the consumer commits offsets manually, fetches one record per
 * poll (an import is a long unit of work) and tolerates long processing through
 * max.poll.interval.ms. Topic partitions bound the total parallelism across all worker
 * instances; the container concurrency bounds it per instance.
 */
@Configuration
public class MessagingConfig {

    @Bean
    ProducerFactory<String, ImportRequestedEvent> importEventProducerFactory(KafkaProperties props) {
        Map<String, Object> cfg = new HashMap<>(props.buildProducerProperties());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        cfg.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(cfg);
    }

    @Bean
    KafkaTemplate<String, ImportRequestedEvent> importEventKafkaTemplate(
            ProducerFactory<String, ImportRequestedEvent> factory) {
        return new KafkaTemplate<>(factory);
    }

    @Bean
    NewTopic importsRequestedTopic(AppProperties props) {
        return TopicBuilder.name(props.kafka().topic())
                .partitions(props.kafka().partitions())
                .replicas(1)
                .build();
    }

    @Bean
    JooqImportJobStore importJobStore(DSLContext db) {
        return new JooqImportJobStore(db);
    }

    @Bean
    ImportJobQueue importJobQueue(KafkaTemplate<String, ImportRequestedEvent> template, JooqImportJobStore store,
                                  ImportRepository imports, Clock clock, AppProperties props) {
        return new KafkaImportJobQueue(template, props.kafka().topic(), props.kafka().publishTimeout(), store, imports,
                clock, props.processing().leaseDuration());
    }

    @Bean
    KafkaHealthIndicator kafkaHealthIndicator(KafkaAdmin admin) {
        return new KafkaHealthIndicator(admin);
    }

    // ---- consumer side (worker role only) -----------------------------------------

    @Bean
    @ConditionalOnProperty(prefix = "app.processing", name = "enabled", havingValue = "true", matchIfMissing = true)
    ConsumerFactory<String, ImportRequestedEvent> importEventConsumerFactory(KafkaProperties props, AppProperties app) {
        Map<String, Object> cfg = new HashMap<>(props.buildConsumerProperties());
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        cfg.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        cfg.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        cfg.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ImportRequestedEvent.class.getName());
        cfg.put(JsonDeserializer.TRUSTED_PACKAGES, ImportRequestedEvent.class.getPackageName());
        cfg.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        cfg.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) app.kafka().maxProcessingTime().toMillis());
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(cfg);
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.processing", name = "enabled", havingValue = "true", matchIfMissing = true)
    ConcurrentMessageListenerContainer<String, ImportRequestedEvent> importEventListenerContainer(
            ConsumerFactory<String, ImportRequestedEvent> factory, ImportWorker worker, AppProperties props) {
        ContainerProperties cp = new ContainerProperties(props.kafka().topic());
        cp.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        cp.setGroupId(props.kafka().consumerGroup());
        cp.setMessageListener(worker);
        cp.setShutdownTimeout(props.shutdown().gracePeriod().toMillis());
        cp.setPollTimeout(1000);
        var container = new ConcurrentMessageListenerContainer<>(factory, cp);
        container.setConcurrency(props.processing().maxConcurrentImports());
        // Poison records (undeserialisable) are logged and skipped, never retried.
        container.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        container.setAutoStartup(false); // the worker starts and stops it in its own lifecycle phase
        worker.attach(container);
        return container;
    }
}
