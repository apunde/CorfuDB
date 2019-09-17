package org.corfudb.runtime.collections;

import java.util.Collection;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by zlokhandwala on 2019-08-10.
 */
@Data
@AllArgsConstructor
public class QueryResult<E> {

    private final Collection<E> result;
}
