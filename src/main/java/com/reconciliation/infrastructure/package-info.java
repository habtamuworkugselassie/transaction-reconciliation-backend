/**
 * Infrastructure layer: adapters that implement application ports using concrete
 * technology (PostgreSQL via JDBC, local filesystem, thread pools, Micrometer).
 *
 * <p>Adapters may depend on {@code application} and {@code domain}; nothing in those
 * layers may depend on this package.
 */
package com.reconciliation.infrastructure;
