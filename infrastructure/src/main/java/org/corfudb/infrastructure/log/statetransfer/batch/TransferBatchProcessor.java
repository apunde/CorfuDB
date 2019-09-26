package org.corfudb.infrastructure.log.statetransfer.batch;

import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferException;
import org.corfudb.runtime.view.RuntimeLayout;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public interface TransferBatchProcessor {

    /**
     * Transfer the batch of addresses.
     * @param addresses The addresses we need to transfer.
     * @param server Source server.
     * @param runtimeLayout RuntimeLayout.
     * @return Future of the result of the transfer, containing maximum transferred address or
     * an exception.
     */
    CompletableFuture<Result<Long, StateTransferException>> transfer(
            List<Long> addresses, String server, RuntimeLayout runtimeLayout
    );

    /**
     * Handle errors that resulted during the transfer.
     * @param transferResult Result of the transfer with a maximum transferred address or an exception.
     * @param runtimeLayout RuntimeLayout.
     * @param retries Number of retries during the error handling.
     * @return Future of the result of the transfer, containing maximum transferred address or
     * an exception.
     */
    CompletableFuture<Result<Long, StateTransferException>> handlePossibleTransferFailures
            (Result<Long, StateTransferException> transferResult, RuntimeLayout runtimeLayout, AtomicInteger retries);
}
