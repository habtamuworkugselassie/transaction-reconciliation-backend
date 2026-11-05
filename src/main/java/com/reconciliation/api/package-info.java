/**
 * HTTP layer: controllers, DTOs and error mapping. Thin by design: it translates
 * requests into use-case calls and domain errors into the public error contract.
 * No business rule lives here; a controller test with a mocked use case proves it.
 */
package com.reconciliation.api;
