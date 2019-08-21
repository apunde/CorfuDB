package org.corfudb.infrastructure.orchestrator.actions;

import lombok.NonNull;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
public class ZeroCopyStateTransfer extends StateTransfer{

    private ZeroCopyStateTransfer(){
        // Hide implicit public constructor
    }


    public static void transfer(Layout layout,
                                @NonNull String endpoint,
                                CorfuRuntime runtime,
                                Layout.LayoutSegment segment) throws InterruptedException{

    }


}
