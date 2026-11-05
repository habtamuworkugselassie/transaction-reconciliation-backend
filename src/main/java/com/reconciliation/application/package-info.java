/**
 * Application layer: use cases and the ports they depend on.
 *
 * <p>Dependency direction is strictly inward: {@code api} and {@code infrastructure}
 * depend on {@code application}, which depends on {@code domain}. Use cases receive
 * every infrastructure dependency (repositories, storage, queue, scorer, clock,
 * id generator, metrics, retry policy) through constructor injection via the
 * interfaces in {@link com.reconciliation.application.port}. They never construct
 * an adapter and never touch Spring, so each use case can be run with in-memory
 * fakes (see {@code CreateImportUseCaseTest}).
 */
package com.reconciliation.application;
