package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.concurrent.SingletonResource;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Map.*;

/**
 * This class is responsible for reading from the remote logunits and writing to the local log.
 */
@Slf4j
@Builder
public class StateTransferWriter {

    private static final int SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT = 60;

    @Getter
    @Setter
    @NonNull
    private ServerContext serverContext;

    @Getter
    @Setter
    @NonNull
    private StreamLog streamLog;

    @Getter
    @Setter
    private SingletonResource<CorfuRuntime> runtimeSingletonResource
            = SingletonResource.withInitial(this::getNewCorfuRuntime);

    private static final ReadOptions readOptions = ReadOptions.builder()
            .waitForHole(true)
            .clientCacheable(false)
            .serverCacheable(false)
            .build();

    private CorfuRuntime getNewCorfuRuntime() {
        CorfuRuntime.CorfuRuntimeParameters params
                = serverContext.getManagementRuntimeParameters();
        params.setCacheDisabled(true);
        params.setSystemDownHandlerTriggerLimit(SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT);
        params.setSystemDownHandler(runtimeSystemDownHandler);

        CorfuRuntime runtime = CorfuRuntime.fromParameters(params);

        // Same layout as a management layout because the runtime-dependent workflows
        // are initiated from the ManagementServer.
        final Layout managementLayout = serverContext.copyManagementLayout();

        if (managementLayout != null) {
            managementLayout.getLayoutServers().forEach(runtime::addLayoutServer);
        }

        runtime.connect();
        log.info("StateTransferWriter: runtime connected");
        return runtime;
    }

    private final Runnable runtimeSystemDownHandler = () -> {
        log.warn("LogStateTransferor: Runtime stalled. Invoking systemDownHandler after {} "
                + "unsuccessful tries.", SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT);
        throw new UnreachableClusterException("Runtime stalled. Invoking systemDownHandler after "
                + SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT + " unsuccessful tries.");
    };


    public void stateTransfer(List<Long> addresses) {
        int readSize = runtimeSingletonResource.get().getParameters().getBulkReadSize();
        RuntimeLayout runtimeLayout = runtimeSingletonResource.get().getLayoutView().getRuntimeLayout();
        Map<String, List<List<Long>>> serversToBatches =
                mapServersToBatches(addresses, readSize, runtimeLayout);

        serversToBatches.entrySet().stream().map(entry -> {
            // process every batch
            String server = entry.getKey();
            List<List<Long>> batches = entry.getValue();
            batches.stream().map(batch -> {

            })

        })

    }

    /**
     * Read garbage -> write garbage -> read records -> write records.
     *
     * @param addresses A batch of consecutive addresses.
     * @param server    A server to read garbage from.
     * @param runtimeLayout A runtime layout to use for connections.
     * @return Result of an empty value if a pipeline executes correctly;
     *         a result containing a first encountered error otherwise.
     */
    public CompletableFuture<Result<Void, StateTransferException>> transferBatch
            (List<Long> addresses, String server, RuntimeLayout runtimeLayout) {
        return readGarbage(addresses, server, runtimeLayout)
                .thenApply(readGarbageResult -> readGarbageResult
                        .flatMap(this::writeRecords))
                .thenApply(writeGarbageResult ->
                        writeGarbageResult.flatMap(x ->
                                readRecords(addresses, runtimeLayout, readOptions)))
                .thenApply(readRecordsResult -> readRecordsResult.flatMap(this::writeRecords));
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

    /**
     * Reads garbage from other log units.
     *
     * @param addresses     The addresses corresponding to the garbage entries.
     * @param server        Target server.
     * @param runtimeLayout A runtime layout to use for connections.
     * @return A completable future of a garbage read response.
     */
    public static CompletableFuture<Result<List<LogData>, StateTransferException>> readGarbage(List<Long> addresses,
                                                                                               String server,
                                                                                               RuntimeLayout
                                                                                                       runtimeLayout) {
        log.trace("Reading garbage for addresses: {}", addresses);
        return runtimeLayout.getLogUnitClient(server)
                .readGarbageEntries(addresses)
                .thenApply(readResponse -> {
                    Map<Long, LogData> readResponseAddresses = readResponse.getAddresses();
                    return handleRead(addresses, readResponseAddresses);
                });
    }

    /**
     * Reads data entries by utilizing the replication protocol.
     *
     * @param addresses     The list of addresses.
     * @param runtimeLayout A runtime layout to use for connections.
     * @param readOptions   The read options to use for the address space view.
     * @return A result of reading records.
     */
    public static Result<List<LogData>, StateTransferException> readRecords(List<Long> addresses,
                                                                            RuntimeLayout runtimeLayout,
                                                                            ReadOptions readOptions) {
        log.trace("Reading data for addresses: {}", addresses);

        AddressSpaceView addressSpaceView = runtimeLayout.getRuntime().getAddressSpaceView();

        return handleRead(addresses, addressSpaceView.read(addresses, readOptions).entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> (LogData) entry.getValue())));
    }


    private static Result<List<LogData>, StateTransferException> handleRead(List<Long> addresses,
                                                                            Map<Long, LogData> readResult) {
        List<Long> transferredAddresses =
                addresses.stream().filter(readResult::containsKey)
                        .collect(Collectors.toList());
        if (transferredAddresses.equals(addresses)) {
            return Result.of(() -> readResult.entrySet().stream()
                    .sorted(Entry.comparingByKey())
                    .map(Entry::getValue).collect(Collectors.toList()));
        } else {
            HashSet<Long> transferredSet = new HashSet<>(transferredAddresses);
            HashSet<Long> entireSet = new HashSet<>(addresses);
            return new Result<>(new ArrayList<>(),
                    new IncompleteReadException(Sets.difference(entireSet, transferredSet)));
        }
    }


    /**
     * Appends data (or garbage) to the streamlog.
     *
     * @param dataEntries The list of entries (data or garbage).
     * @return A result of a record append.
     */
    public Result<Void, StateTransferException> writeRecords(List<LogData> dataEntries) {
        log.trace("Writing data entries: {}", dataEntries);

        Result<Void, RuntimeException> result = Result.of(() -> {
            streamLog.append(dataEntries);
            return null;
        });

        return result.mapError(x -> new RejectedAppendException(dataEntries));
    }

}
