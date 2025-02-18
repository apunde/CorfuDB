package org.corfudb.runtime.collections;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;

/**
 *
 * Created by sneginhal on 10/22/2019.
 */
@Slf4j
public class TransactionPoller implements Runnable {

    /**
     * Corfu Runtime.
     */
    private final CorfuRuntime runtime;

    /**
     * List of StreamingSubscriptionContexts to process.
     *
     * This is the list of StreamingSubscriptionContexts sorted by the last read addresses.
     */
    private final List<StreamingSubscriptionContext> streamContexts;

    /**
     * Constructor.
     *
     * @param streams The list of StreamingSubscriptionContexts to process.
     */
    public TransactionPoller(@Nonnull CorfuRuntime runtime,
            @Nonnull List<StreamingSubscriptionContext> streams) {
        this.runtime = runtime;
        this.streamContexts = streams.stream()
                .sorted(Comparator.comparingLong(sc -> sc.getLastReadAddress()))
                .collect(Collectors.toList());
    }

    /**
     *
     */
    @Override
    public void run() {
        try {
            pollTxStream();
        } catch (Throwable t) {
            // We don't want the thread running the poller to be terminated due to
            // some unexpected exception, so catch all here.
            log.error("Encountered an exception while polling the txn stream: {}", t);

            streamContexts.forEach(sc -> sc.setPollerException(t));
        } finally {
            streamContexts.forEach(sc -> sc.release());
        }
    }

    private void pollTxStream() {
 
        long lastReadAddress = streamContexts.get(0).getLastReadAddress();

        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(true)
                .cacheEntries(false)
                .build();
        IStreamView txStream = runtime.getStreamsView()
                .get(ObjectsView.TRANSACTION_STREAM_ID, options);
        log.trace("Seeking txStream to {}", lastReadAddress + 1);
        txStream.seek(lastReadAddress + 1);
        log.trace("txStream current global position after seeking {}, hasNext {}",
                txStream.getCurrentGlobalPosition(), txStream.hasNext());

        List<ILogData> updates = txStream.remaining();

        log.trace("{} updates remaining in the txStream", updates.size());

        for (ILogData update : updates) {
            List<StreamingSubscriptionContext> streamContextsNotUpdated = new LinkedList<>();
            for (StreamingSubscriptionContext sc : streamContexts) {
                if (!sc.enqueueStreamEntry(lastReadAddress, update)) {
                    // If enqueue fails, further enqueues will fail as the
                    // lastReadAddress won't match.
                    streamContextsNotUpdated.add(sc);
                }
            }
            lastReadAddress = update.getGlobalAddress();
            // Release the lock on StreamingSubscriptionContexts that were not updated and remove
            // them from the list of StreamingSubscriptionContexts being processed.
            streamContextsNotUpdated.forEach(sc -> sc.release());
            streamContexts.removeAll(streamContextsNotUpdated);
        }
    }
}
