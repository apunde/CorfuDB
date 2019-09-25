package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatch;
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
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
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

import static java.util.Map.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentStateTransferState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentStateTransferState.TRANSFERRED;

/**
 * This class is responsible for reading from the remote log units and writing to the local log.
 */
@Slf4j
@Builder
public class StateTransferWriter {

    @Getter
    @NonNull
    private TransferBatch transferBatch;

    @Getter
    @NonNull
    private CorfuRuntime corfuRuntime;

    public void stateTransfer(List<Long> addresses,
                              CurrentTransferSegment segment,
                              Map<CurrentTransferSegment,
                                      CurrentTransferSegmentStatus> statusMap) {
        int readSize = corfuRuntime.getParameters().getBulkReadSize();
        RuntimeLayout runtimeLayout = corfuRuntime.getLayoutView().getRuntimeLayout();
        Map<String, List<List<Long>>> serversToBatches =
                mapServersToBatches(addresses, readSize, runtimeLayout);

        CFUtils.sequence(serversToBatches.entrySet().stream().map(entry -> {
            // Process every batch, handling errors if any, updating state,
            // propagating to the caller if the timeout occurs,
            // the retries are exhausted, or unexpected error happened.
            String server = entry.getKey();
            List<List<Long>> batches = entry.getValue();
            List<CompletableFuture<Void>> batchTransferResult = batches.stream().map(batch ->
                    transferBatch.transfer(batch, server, runtimeLayout)
                    .thenCompose(transferResult ->
                            transferBatch.handlePossibleTransferFailures(
                                    transferResult,
                                    runtimeLayout,
                                    new AtomicInteger()))
                    .thenCompose(transferResult ->
                            updateSegmentState(transferResult, segment, statusMap)))
                    .collect(Collectors.toList());
            return CFUtils.sequence(batchTransferResult);
        }).collect(Collectors.toList())).join();

    }

    private CompletableFuture<Void> updateSegmentState(
            Result<Long, StateTransferException> transferResult, CurrentTransferSegment segment,
            Map<CurrentTransferSegment, CurrentTransferSegmentStatus> statusMap
    ){
        if(transferResult.isValue()){
            statusMap.computeIfPresent(segment, (seg, status) -> {
                long maxTransferredAddress = transferResult.get();
                status.setLastTransferredAddress(maxTransferredAddress);
                if(maxTransferredAddress == segment.getEndAddress()){
                    status.setSegmentStateTransferState(TRANSFERRED);
                }
                return status;
            } );
        }
        else{
            log.error("Unrecoverable transfer error occurred: ", transferResult.getError());
            statusMap.computeIfPresent(segment, (seg, status) -> {
                status.setSegmentStateTransferState(FAILED);
                return status;
            });
            // If the unrecoverable error occurs -> short circuit the transfer.
            throw transferResult.getError();
        }
        return null;
    }

    /**
     * Creates a map from servers to the address batches they are responsible for.
     *
     * @param addresses     The addresses of garbage or data entries.
     * @param bulkSize      The size of a batch, small enough to safely transfer within one rpc call.
     * @param runtimeLayout The current runtime layout to extract a server information.
     * @return A map from servers to list of address batches.
     */
    @VisibleForTesting
    private static Map<String, List<List<Long>>> mapServersToBatches(List<Long> addresses,
                                                                     int bulkSize,
                                                                     RuntimeLayout runtimeLayout) {

        Map<String, List<Long>> serverToAddresses = addresses.stream().map(address -> {
            List<String> servers = runtimeLayout
                    .getLayout()
                    .getStripe(address)
                    .getLogServers();
            String logServer = servers.get(servers.size() - 1);
            return new SimpleEntry<>(logServer, address);
        }).collect(Collectors.groupingBy(SimpleEntry::getKey,
                Collectors.mapping(SimpleEntry::getValue, Collectors.toList())));

        return serverToAddresses
                .entrySet()
                .stream()
                .map(entry -> new SimpleEntry<>(entry.getKey(),
                        Lists.partition(entry.getValue(), bulkSize)))
                .collect(Collectors.toMap(SimpleEntry::getKey,
                        SimpleEntry::getValue));
    }
}
