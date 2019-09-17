package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import lombok.Data;

/**
 * Created by zlokhandwala on 2019-08-15.
 */
@Data
public class CorfuRecord<V extends Message, M extends Message> {
    private final V payload;
    private final M metadata;
}
