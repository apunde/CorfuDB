package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.concurrent.SingletonResource;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
@Builder
public class LogStateTransferor {

    public enum StateTransferState{
        TRANSFERRING,
        TRANSFERRED
    }

    private static final int SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT = 60;

    @AllArgsConstructor
    @Getter
    private static class CurrentTransferSegment{
        private final long startAddress;
        private final long endAddress;
        private long transferredUpTo;
    }

    @Getter
    @Setter
    private StateTransferState state;

    @Getter
    @Setter
    private CurrentTransferSegment currentTransferSegment;

    @Getter
    @Setter
    private StreamLog streamLog;

    @Getter
    @Setter
    private ServerContext serverContext;

    @Getter
    @Setter
    private SingletonResource<CorfuRuntime> runtimeSingletonResource
            = SingletonResource.withInitial(this::getNewCorfuRuntime);

    private CorfuRuntime getNewCorfuRuntime(){
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
        log.info("LogStateTransferor: runtime connected");
        return runtime;
    }

    // api entry
    public <T>

    private List<Long> getUnknownAddressesInRange(long rangeStart, long rangeEnd) {

        Set<Long> knownAddresses = streamLog.getKnownAddressesInRange(rangeStart, rangeEnd);
        List<Long> unknownAddresses = new ArrayList<>();
        for (long address = rangeStart; address <= rangeEnd; address++) {
            if (!knownAddresses.contains(address)) {
                unknownAddresses.add(address);
            }
        }
        return unknownAddresses;
    }

    private final Runnable runtimeSystemDownHandler = () -> {
        log.warn("LogStateTransferor: Runtime stalled. Invoking systemDownHandler after {} "
                + "unsuccessful tries.", SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT);
        throw new UnreachableClusterException("Runtime stalled. Invoking systemDownHandler after "
                + SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT + " unsuccessful tries.");
    };


    /** Creates a map from servers to the address batches they are responsible for.
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
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    /**
     * Reads garbage from other log units in parallel.
     *
     * @param addresses     The addresses corresponding to the garbage entries.
     * @param bulkReadSize  The number of records to read within one RPC call.
     * @param runtimeLayout A runtime layout to use for connections.
     * @return A completable future of a list of garbage responses.
     */
    public static synchronized CompletableFuture<List<ReadResponse>> readGarbage(List<Long> addresses,
                                                                                  int bulkReadSize,
                                                                                  RuntimeLayout
                                                                                          runtimeLayout) {
        log.trace("Reading garbage for addresses: {}", addresses);
        Map<String, List<List<Long>>> serverToGarbageAddresses =
                mapServersToBatches(addresses, bulkReadSize, runtimeLayout);

        List<CompletableFuture<ReadResponse>> garbageResponses = serverToGarbageAddresses
                .entrySet()
                .stream()
                .flatMap(entry -> entry.getValue()
                        .stream()
                        .map(batch ->
                                runtimeLayout
                                        .getLogUnitClient(entry.getKey())
                                        .readGarbageEntries(batch)))
                .collect(Collectors.toList());
        return CFUtils.sequence(garbageResponses);
    }

    /**
     * Reads data entries by utilizing the replication protocol.
     *
     * @param addresses        The list of addresses.
     * @param bulkReadSize     The number of records to read within one RPC call.
     * @param runtimeLayout    A runtime layout to use for connections.
     * @param addressSpaceView A view of the address space to call a read via a protocol.
     * @param readOptions      The read options to use for the address space view.
     * @return A map from address to the LogData entry.
     */
    @VisibleForTesting
    public static synchronized Map<Long, ILogData> readRecords(List<Long> addresses,
                                                                int bulkReadSize,
                                                                RuntimeLayout runtimeLayout,
                                                                AddressSpaceView addressSpaceView,
                                                                ReadOptions readOptions) {
        log.trace("Reading data entries: {}", addresses);
        Map<String, List<List<Long>>> serverToAddresses =
                mapServersToBatches(addresses, bulkReadSize, runtimeLayout);

        return serverToAddresses
                .values()
                .stream()
                .flatMap(batches ->
                        batches.stream().flatMap
                                (batch -> addressSpaceView
                                        .read(batch, readOptions)
                                        .entrySet()
                                        .stream()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    /**
     * Write records to the current stream log.
     *
     * @param dataEntries The list of entries (data or garbage).
     * @param streamLog   The instance of the underlying stream log.
     */
    public static synchronized void writeRecords(List<LogData> dataEntries, StreamLog streamLog) {
        log.trace("Writing data entries: {}", dataEntries);
        streamLog.append(dataEntries);
    }
}
