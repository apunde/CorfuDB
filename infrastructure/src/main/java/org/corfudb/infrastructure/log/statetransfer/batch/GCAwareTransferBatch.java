package org.corfudb.infrastructure.log.statetransfer.batch;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.exceptions.IncompleteDataReadException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.IncompleteGarbageReadException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.IncompleteReadException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.RejectedAppendException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.RejectedDataException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.RejectedGarbageException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferFailure;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.Sleep;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;


@Slf4j
@Builder
public class GCAwareTransferBatch implements TransferBatch {


    private static final int MAX_RETRIES = 3;
    private static final int MAX_WRITE_MILLISECONDS_TIMEOUT = 1000;
    private static final Duration MAX_RETRY_TIMEOUT = Duration.ofSeconds(10);
    private static final float RANDOM_FACTOR_BACKOFF = 0.5f;

    @Getter
    @NonNull
    private StreamLog streamLog;

    @Getter
    private static final ReadOptions readOptions = ReadOptions.builder()
            .waitForHole(true)
            .clientCacheable(false)
            .serverCacheable(false)
            .build();

    @Override
    public CompletableFuture<Result<Long, StateTransferException>> handlePossibleTransferFailures
            (Result<Long, StateTransferException> transferResult, RuntimeLayout runtimeLayout, AtomicInteger retries) {
        if (transferResult.isError()) {
            StateTransferException error = transferResult.getError();
            if (error instanceof IncompleteReadException) {
                IncompleteReadException incompleteReadException = (IncompleteReadException) error;
                return handlePossibleTransferFailures(tryHandleIncompleteRead(incompleteReadException, runtimeLayout, retries)
                        .join(), runtimeLayout, retries);

            } else if (error instanceof RejectedAppendException) {
                RejectedAppendException rejectedAppendException = (RejectedAppendException) error;
                return handlePossibleTransferFailures(tryHandleRejectedWrite(rejectedAppendException, runtimeLayout, retries)
                        .join(), runtimeLayout, retries);
            } else {
                Result<Long, StateTransferException> stateTransferFailureResult =
                        transferResult.mapError(e -> new StateTransferFailure());
                return CompletableFuture.completedFuture(stateTransferFailureResult);
            }
        } else {
            return CompletableFuture.completedFuture(transferResult);
        }
    }

    private CompletableFuture<Result<Long, StateTransferException>> tryHandleIncompleteRead
            (IncompleteReadException incompleteReadException, RuntimeLayout runtimeLayout, AtomicInteger readRetries) {

        try {
            CompletableFuture<Result<Long, StateTransferException>> retryResult =
                    IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {

                        // Get a pipeline function.
                        Supplier<CompletableFuture<Result<Long, StateTransferException>>> pipeline =
                                getErrorHandlingPipeline(incompleteReadException, runtimeLayout);
                        // Extract the result.
                        Result<Long, StateTransferException> joinResult = pipeline.get().join();
                        if (joinResult.isError()) {

                            // If an error occurred, increment retries.
                            readRetries.incrementAndGet();

                            // If an error happened due to the rejected append, handle it differently, return.
                            if (joinResult.getError() instanceof RejectedAppendException) {
                                return CompletableFuture.completedFuture(joinResult);
                                // If the instance of an error is the same, retry with exp. backoff
                                // if possible, otherwise, stop.
                            } else if (joinResult.getError() instanceof IncompleteReadException) {
                                if (readRetries.get() >= MAX_RETRIES) {
                                    throw new RetryExhaustedException("Read retries are exhausted");
                                } else {
                                    log.warn("Retried {} times", readRetries.get());
                                    throw new RetryNeededException();
                                }
                                // If an error happened for the unknown reason, stop.
                            } else {
                                // Unhandled error, return.
                                return CompletableFuture.completedFuture(joinResult);
                            }
                        } else {
                            // If the result is not an error, return.
                            return CompletableFuture.completedFuture(joinResult);
                        }

                    }).setOptions(retry -> {
                        retry.setMaxRetryThreshold(MAX_RETRY_TIMEOUT);
                        retry.setRandomPortion(RANDOM_FACTOR_BACKOFF);
                    }).run();

            // Map to unrecoverable error in case retries are failed or unhandled error occurred.
            return CompletableFuture.completedFuture(retryResult.join()
                    .mapError(e -> new StateTransferFailure()));
            // Map to unrecoverable error if an interrupt has occurred.
        } catch (InterruptedException ie) {
            return CompletableFuture.completedFuture(Result.error(new StateTransferFailure()));
        }
    }

    private CompletableFuture<Result<Long, StateTransferException>> tryHandleRejectedWrite
            (RejectedAppendException rejectedAppendException, RuntimeLayout runtimeLayout, AtomicInteger writeRetries) {

        if(writeRetries.get() < MAX_RETRIES){
            // Get a pipeline function.
            Supplier<CompletableFuture<Result<Long, StateTransferException>>> pipeline
                    = getErrorHandlingPipeline(rejectedAppendException, runtimeLayout);
            // Extract the result.
            Result<Long, StateTransferException> joinResult = pipeline.get().join();

            // If the result is an error, increment retries.
            if (joinResult.isError()) {
                writeRetries.incrementAndGet();
                // If an error happened due to the IncompleteRead, handle it differently, return.
                if (joinResult.getError() instanceof IncompleteReadException) {
                    return CompletableFuture.completedFuture(joinResult);
                    // If the instance of the error is the same, sleep, recurse.
                } else if (joinResult.getError() instanceof RejectedAppendException) {
                    Sleep.sleepUninterruptibly(Duration.ofMillis(MAX_WRITE_MILLISECONDS_TIMEOUT));
                    return tryHandleRejectedWrite(rejectedAppendException, runtimeLayout, writeRetries);
                }
                // If an error happened for the unknown reason, stop.
                else {
                    return CompletableFuture.completedFuture(Result.error(new StateTransferFailure()));
                }
                // If the result is not an error, return.
            } else {
                return CompletableFuture.completedFuture(joinResult);
            }
        }
        // Map to unrecoverable error in case retries are failed.
        else{
            return CompletableFuture.completedFuture(Result.error(new StateTransferFailure()));
        }
    }

    private Supplier<CompletableFuture<Result<Long, StateTransferException>>> getErrorHandlingPipeline
            (StateTransferException exception, RuntimeLayout runtimeLayout) {
        if (exception instanceof IncompleteReadException) {
            IncompleteReadException incompleteReadException = (IncompleteReadException) exception;
            List<Long> missingAddresses =
                    Ordering.natural().sortedCopy(incompleteReadException.getMissingAddresses());

            if (incompleteReadException instanceof IncompleteGarbageReadException) {
                IncompleteGarbageReadException incompleteGarbageReadException =
                        (IncompleteGarbageReadException) incompleteReadException;
                return () -> transfer(missingAddresses,
                        incompleteGarbageReadException.getHostAddress(), runtimeLayout);
            } else {
                return () -> CompletableFuture.supplyAsync(() ->
                        readRecords(missingAddresses, runtimeLayout))
                        .thenApply(result -> result.flatMap(this::writeData));
            }

        } else {
            RejectedAppendException rejectedAppendException = (RejectedAppendException) exception;
            List<LogData> missingData = rejectedAppendException.getDataEntries();
            if (rejectedAppendException instanceof RejectedDataException) {
                return () -> CompletableFuture.supplyAsync(() -> writeData(missingData));
            } else {
                List<Long> missingAddresses = missingData.stream()
                        .map(IMetadata::getGlobalAddress).collect(Collectors.toList());
                return () -> CompletableFuture
                        .supplyAsync(() -> writeGarbage(missingData))
                        .thenApply(garbageWriteResult -> garbageWriteResult
                                .flatMap(result -> readRecords(missingAddresses, runtimeLayout)))
                        .thenApply(dataReadResult -> dataReadResult.flatMap(this::writeData));
            }
        }
    }

    /**
     * Read garbage -> write garbage -> read data -> write data.
     *
     * @param addresses     A batch of consecutive addresses.
     * @param server        A server to read garbage from.
     * @param runtimeLayout A runtime layout to use for connections.
     * @return Result of an empty value if a pipeline executes correctly;
     * a result containing the first encountered error otherwise.
     */
    @Override
    public CompletableFuture<Result<Long, StateTransferException>> transfer
    (List<Long> addresses, String server, RuntimeLayout runtimeLayout) {
        return readGarbage(addresses, server, runtimeLayout)
                .thenApply(readGarbageResult -> readGarbageResult
                        .flatMap(this::writeGarbage))
                .thenApply(writeGarbageResult ->
                        writeGarbageResult.flatMap(x ->
                                readRecords(addresses, runtimeLayout)))
                .thenApply(readDataResult -> readDataResult.flatMap(this::writeData));
    }

    /**
     * Reads garbage from other log units.
     *
     * @param addresses     The addresses corresponding to the garbage entries.
     * @param server        Target server.
     * @param runtimeLayout A runtime layout to use for connections.
     * @return A completable future of a garbage read response.
     */
    private static CompletableFuture<Result<List<LogData>, StateTransferException>> readGarbage(List<Long> addresses,
                                                                                                String server,
                                                                                                RuntimeLayout
                                                                                                        runtimeLayout) {
        log.trace("Reading garbage for addresses: {}", addresses);
        return runtimeLayout.getLogUnitClient(server)
                .readGarbageEntries(addresses)
                .thenApply(readResponse -> {
                    Map<Long, LogData> readResponseAddresses = readResponse.getAddresses();
                    return handleRead(addresses, readResponseAddresses)
                            .mapError(e ->
                                    new IncompleteGarbageReadException(server,
                                            e.getMissingAddresses()));
                });
    }

    /**
     * Reads data entries by utilizing the replication protocol.
     *
     * @param addresses     The list of addresses.
     * @param runtimeLayout A runtime layout to use for connections.
     * @return A result of reading records.
     */
    private static Result<List<LogData>, StateTransferException> readRecords(List<Long> addresses,
                                                                             RuntimeLayout runtimeLayout) {
        log.trace("Reading data for addresses: {}", addresses);

        AddressSpaceView addressSpaceView = runtimeLayout.getRuntime().getAddressSpaceView();

        return handleRead(addresses,
                addressSpaceView.read(addresses, readOptions).entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> (LogData) entry.getValue())))
                .mapError(e -> new IncompleteDataReadException(e.getMissingAddresses()));
    }

    private Result<Long, StateTransferException> writeGarbage(List<LogData> garbageEntries) {
        log.trace("Writing garbage entries: {}", garbageEntries);
        return writeRecords(garbageEntries).mapError(e -> new RejectedGarbageException(e.getDataEntries()));
    }

    private Result<Long, StateTransferException> writeData(List<LogData> dataEntries) {
        log.trace("Writing data entries: {}", dataEntries);
        return writeRecords(dataEntries).mapError(e -> new RejectedDataException(e.getDataEntries()));
    }

    /**
     * Appends data (or garbage) to the stream log.
     *
     * @param dataEntries The list of entries (data or garbage).
     * @return A result of a record append, containing the max written address.
     */
    private Result<Long, RejectedAppendException> writeRecords(List<LogData> dataEntries) {

        Result<Long, RuntimeException> result = Result.of(() -> {
            streamLog.append(dataEntries);
            Optional<Long> maxWrittenAddress =
                    dataEntries.stream()
                            .map(IMetadata::getGlobalAddress)
                            .max(Comparator.comparing(Long::valueOf));
            // Should be present as we've checked it during the previous stages.
            return maxWrittenAddress.orElse(null);
        });

        return result.mapError(x -> new RejectedAppendException(dataEntries));
    }

    private static Result<List<LogData>, IncompleteReadException> handleRead(List<Long> addresses,
                                                                             Map<Long, LogData> readResult) {
        List<Long> transferredAddresses =
                addresses.stream().filter(readResult::containsKey)
                        .collect(Collectors.toList());
        if (transferredAddresses.equals(addresses)) {
            return Result.of(() -> readResult.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(Map.Entry::getValue).collect(Collectors.toList()));
        } else {
            HashSet<Long> transferredSet = new HashSet<>(transferredAddresses);
            HashSet<Long> entireSet = new HashSet<>(addresses);
            return new Result<>(new ArrayList<>(),
                    new IncompleteReadException(Sets.difference(entireSet, transferredSet)));
        }
    }

}
