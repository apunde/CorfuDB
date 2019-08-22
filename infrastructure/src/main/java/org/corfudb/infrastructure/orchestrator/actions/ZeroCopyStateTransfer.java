package org.corfudb.infrastructure.orchestrator.actions;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ZeroCopyStateTransfer extends StateTransfer {

    private ZeroCopyStateTransfer() {
        // Hide implicit public constructor
    }


    public static void transfer(Layout layout,
                                @NonNull String currentEndpoint,
                                @NonNull String remoteEndpoint,
                                CorfuRuntime runtime,
                                Layout.LayoutSegment segment) throws InterruptedException {

        long trimMark = setTrimOnNewLogUnit(layout, runtime, remoteEndpoint);
        if (trimMark > segment.getEnd()) {
            log.info("stateTransfer: Nothing to transfer, trimMark {}"
                            + "greater than end of segment {}",
                    trimMark, segment.getEnd());
            return;
        }

        final long segmentStart = Math.max(trimMark, segment.getStart());
        final long segmentEnd = segment.getEnd() - 1;
        log.info("stateTransfer: Total address range to transfer: [{}-{}] to node {}",
                segmentStart, segmentEnd, remoteEndpoint);

        // get missing entries since its remote
//        List<Long> chunks = getMissingEntriesChunk(layout, runtime, remoteEndpoint,
//                segmentStart, segmentEnd);

        log.info("Calling open socket from thread: {}", Thread.currentThread().getName());
        // open socket on the remote logunit and accept

        ExecutorService ec = Executors.newSingleThreadExecutor();
        ec.submit(() -> runtime.getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(remoteEndpoint)
                .openServerSocketChannel());

        log.info("Sleeping for a bit");
        Thread.sleep(2000);

        log.info("Calling initiate transfer");

        // start transfer from the current log unit
        runtime.getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(currentEndpoint)
                .initiateTransfer(remoteEndpoint).join();
    }


}
