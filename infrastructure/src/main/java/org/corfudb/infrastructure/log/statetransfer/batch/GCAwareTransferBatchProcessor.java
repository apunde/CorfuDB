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
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.RuntimeLayout;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.replication.IReplicationProtocol.*;


@Slf4j
@Builder
public class GCAwareTransferBatchProcessor extends TransferBatchProcessor {


    public GCAwareTransferBatchProcessor(StreamLog streamLog) {
        super(streamLog);
    }

    @Override
    public Supplier<CompletableFuture<Result<Long, StateTransferException>>> getErrorHandlingPipeline
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

                    Map<Long, ILogData> readResponseAddresses =
                            readResponse.getAddresses().entrySet().stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey,
                                            entry -> (ILogData) entry.getValue()));

                    return handleRead(addresses, readResponseAddresses)
                            .mapError(e ->
                                    new IncompleteGarbageReadException(server,
                                            e.getMissingAddresses()));
                });
    }

    /**
     * Reads data entries by utilizing the replication protocol,
     * also writes the highest compaction mark.
     *
     * @param addresses     The list of addresses.
     * @param runtimeLayout A runtime layout to use for connections.
     * @return A result of reading records.
     */

    public Result<List<LogData>, StateTransferException> readRecords(List<Long> addresses,
                                                                     RuntimeLayout runtimeLayout) {
        log.trace("Reading data for addresses: {}", addresses);

        AddressSpaceView addressSpaceView = runtimeLayout.getRuntime().getAddressSpaceView();
        ReadResult readResult = addressSpaceView.fetchAllWithCompactionMark(addresses, readOptions);
        // Update compaction mark using the max remote compaction mark.
        streamLog.updateGlobalCompactionMark(readResult.getCompactionMark());
        return handleRead(addresses, readResult.getData())
                .mapError(e -> new IncompleteDataReadException(e.getMissingAddresses()));
    }

    private Result<Long, StateTransferException> writeGarbage(List<LogData> garbageEntries) {
        log.trace("Writing garbage entries: {}", garbageEntries);
        return writeRecords(garbageEntries).mapError(e -> new RejectedGarbageException(e.getDataEntries()));
    }
}
