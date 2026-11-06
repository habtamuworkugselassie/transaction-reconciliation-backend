package com.reconciliation.config;

import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.infrastructure.messaging.ImportRequestedEvent;
import com.reconciliation.infrastructure.messaging.InvalidImportEventException;
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
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.util.LinkedHashMap;

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
        KafkaTemplate<String, ImportRequestedEvent> template = new KafkaTemplate<>(factory);
        // Producer span + W3C trace headers on the record, so the worker continues the trace.
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    NewTopic importsRequestedTopic(AppProperties props) {
        return TopicBuilder.name(props.kafka().topic())
                .partitions(props.kafka().partitions())
                .replicas(1)
                .build();
    }

    /**
     * Dead-letter topic ({@code <topic>.DLT}) for records the worker cannot use at all:
     * bytes that are not an {@link ImportRequestedEvent}, or an event without an import id.
     * Same partition count as the main topic, because a record goes to the partition it
     * came from.
     */
    @Bean
    NewTopic importsDeadLetterTopic(AppProperties props) {
        return TopicBuilder.name(deadLetterTopic(props))
                .partitions(props.kafka().partitions())
                .replicas(1)
                .build();
    }

    static String deadLetterTopic(AppProperties props) {
        return props.kafka().topic() + ".DLT";
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

    /**
     * Producer for the dead-letter topic. A dead record is either the raw bytes that failed
     * to deserialise (restored from the exception) or a parsed event that was invalid, so
     * values are serialised by type.
     */
    @Bean
    @ConditionalOnProperty(prefix = "app.processing", name = "enabled", havingValue = "true", matchIfMissing = true)
    KafkaTemplate<Object, Object> deadLetterKafkaTemplate(KafkaProperties props) {
        Map<String, Object> cfg = new HashMap<>(props.buildProducerProperties());
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        Map<Class<?>, org.apache.kafka.common.serialization.Serializer<?>> byType = new LinkedHashMap<>();
        byType.put(byte[].class, new ByteArraySerializer());
        byType.put(String.class, new StringSerializer());
        JsonSerializer<Object> json = new JsonSerializer<>();
        json.setAddTypeInfo(false);
        byType.put(Object.class, json);
        DelegatingByTypeSerializer serializer = new DelegatingByTypeSerializer(byType, true);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(cfg, serializer, serializer));
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.processing", name = "enabled", havingValue = "true", matchIfMissing = true)
    ConcurrentMessageListenerContainer<String, ImportRequestedEvent> importEventListenerContainer(
            ConsumerFactory<String, ImportRequestedEvent> factory, ImportWorker worker, AppProperties props,
            KafkaTemplate<Object, Object> deadLetterKafkaTemplate, MeterRegistry meters) {
        ContainerProperties cp = new ContainerProperties(props.kafka().topic());
        cp.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        cp.setGroupId(props.kafka().consumerGroup());
        cp.setMessageListener(worker);
        cp.setShutdownTimeout(props.shutdown().gracePeriod().toMillis());
        cp.setPollTimeout(1000);
        cp.setObservationEnabled(true); // consumer span, parented by the producer's trace headers
        var container = new ConcurrentMessageListenerContainer<>(factory, cp);
        container.setConcurrency(props.processing().maxConcurrentImports());
        // Poison records are never retried (they cannot succeed). They are published to the
        // dead-letter topic with Spring Kafka's exception headers (class, message, stack,
        // original topic/partition/offset), counted, and their offset is committed. The
        // database stays the source of truth: if a real import's event was lost this way, the
        // worker's sweep re-publishes it from its pending row.
        String dlt = deadLetterTopic(props);
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(deadLetterKafkaTemplate,
                (record, ex) -> new TopicPartition(dlt, record.partition()));
        DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, ex) -> {
            meters.counter("kafka.dead.letter.records", "reason", deadLetterReason(ex)).increment();
            recoverer.accept(record, ex);
        }, new FixedBackOff(0L, 0L));
        errorHandler.setCommitRecovered(true);
        container.setCommonErrorHandler(errorHandler);
        container.setAutoStartup(false); // the worker starts and stops it in its own lifecycle phase
        worker.attach(container);
        return container;
    }

    /** Low-cardinality label for the dead-letter counter. */
    static String deadLetterReason(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof DeserializationException) {
                return "deserialization";
            }
            if (t instanceof InvalidImportEventException) {
                return "invalid_event";
            }
        }
        return "other";
    }
}
