package com.reconciliation.config;

import com.reconciliation.api.ImportController;
import com.reconciliation.api.ImportProgressStreamer;
import com.reconciliation.api.RequestIdFilter;
import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.FileStorage;
import com.reconciliation.application.port.IdGenerator;
import com.reconciliation.application.port.IdempotencyCache;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.port.MetricsRecorder;
import com.reconciliation.application.port.RejectionRepository;
import com.reconciliation.application.port.SummaryCache;
import com.reconciliation.application.port.SummaryRepository;
import com.reconciliation.application.port.UploadLimiter;
import com.reconciliation.application.port.UploadRateLimiter;
import com.reconciliation.application.usecase.CancelImportUseCase;
import com.reconciliation.application.usecase.CreateImportUseCase;
import com.reconciliation.application.usecase.GetImportSummaryUseCase;
import com.reconciliation.application.usecase.GetImportUseCase;
import com.reconciliation.application.usecase.ListRejectionsUseCase;
import com.reconciliation.application.usecase.RetryImportUseCase;
import com.reconciliation.infrastructure.concurrency.SemaphoreUploadLimiter;
import com.reconciliation.infrastructure.storage.LocalFileStorage;
import com.reconciliation.infrastructure.time.RandomUuidGenerator;
import com.reconciliation.infrastructure.time.SystemClock;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Composition root: the only place that knows which concrete adapter satisfies
 * which port. Use cases are assembled here from ports; nothing else in the
 * codebase calls {@code new} on an infrastructure class.
 */
@Configuration
public class ApplicationAssembly {

    // ---- cross-cutting ports ----------------------------------------------

    @Bean
    Clock clock() {
        return new SystemClock();
    }

    @Bean
    IdGenerator idGenerator() {
        return new RandomUuidGenerator();
    }

    // ---- infrastructure adapters -----------------------------------------

    @Bean
    FileStorage fileStorage(AppProperties props) throws IOException {
        return new LocalFileStorage(Path.of(props.storage().root()));
    }

    @Bean
    UploadLimiter uploadLimiter(AppProperties props) {
        return new SemaphoreUploadLimiter(props.upload().maxConcurrentUploads());
    }

    // ---- use cases ---------------------------------------------------------

    @Bean
    CreateImportUseCase createImportUseCase(ImportRepository imports, FileStorage storage, ImportJobQueue queue,
                                            UploadLimiter uploadLimiter, Clock clock, IdGenerator ids,
                                            MetricsRecorder metrics, IdempotencyCache idempotencyCache,
                                            UploadRateLimiter rateLimiter, AppProperties props) {
        return new CreateImportUseCase(imports, storage, queue, uploadLimiter, clock, ids, metrics, idempotencyCache,
                props.cache().idempotencyTtl(), props.upload().maxFileSizeBytes(), rateLimiter);
    }

    @Bean
    RetryImportUseCase retryImportUseCase(ImportRepository imports, ImportJobQueue queue, SummaryCache cache,
                                          MetricsRecorder metrics, Clock clock, AppProperties props) {
        return new RetryImportUseCase(imports, queue, cache, metrics, clock, props.processing().maxManualRetries());
    }

    @Bean
    GetImportUseCase getImportUseCase(ImportRepository imports) {
        return new GetImportUseCase(imports);
    }

    @Bean
    CancelImportUseCase cancelImportUseCase(ImportRepository imports, Clock clock) {
        return new CancelImportUseCase(imports, clock);
    }

    @Bean
    GetImportSummaryUseCase getImportSummaryUseCase(ImportRepository imports, SummaryRepository summaries,
                                                    SummaryCache cache, Clock clock, AppProperties props) {
        return new GetImportSummaryUseCase(imports, summaries, cache, props.cache().summaryLiveTtl(),
                props.cache().summaryFinalTtl(), clock);
    }

    @Bean
    ListRejectionsUseCase listRejectionsUseCase(ImportRepository imports, RejectionRepository rejections) {
        return new ListRejectionsUseCase(imports, rejections);
    }

    // ---- HTTP ---------------------------------------------------------------

    @Bean(destroyMethod = "close")
    ImportProgressStreamer importProgressStreamer(GetImportUseCase get, AppProperties props) {
        var e = props.events();
        return new ImportProgressStreamer(get, e.maxConnections(), e.pollInterval(), e.heartbeatInterval(), e.timeout());
    }

    @Bean
    ImportController importController(CreateImportUseCase create, GetImportUseCase get, CancelImportUseCase cancel,
                                      GetImportSummaryUseCase summary, ListRejectionsUseCase rejections,
                                      RetryImportUseCase retry, ImportProgressStreamer streamer, AppProperties props) {
        return new ImportController(create, get, cancel, summary, rejections, retry, streamer,
                props.upload().providerHeader(), props.upload().defaultProvider());
    }

    @Bean
    FilterRegistrationBean<RequestIdFilter> requestIdFilter() {
        FilterRegistrationBean<RequestIdFilter> bean = new FilterRegistrationBean<>(new RequestIdFilter());
        bean.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return bean;
    }
}
