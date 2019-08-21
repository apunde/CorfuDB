package org.corfudb.benchmarks.infrastructure.log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.infrastructure.DataStore;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogDataStore;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.util.serializer.Serializers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@State(Scope.Thread)
public class StreamLogFilesBenchmark {


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(StreamLogFilesBenchmark.class.getSimpleName())
                //.resultFormat(ResultFormatType.CSV)
                //.addProfiler(GCProfiler.class)
                .build();

        new Runner(opt).run();
    }

    LogData getEntry(long x) {
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogData ld = new LogData(DataType.DATA, b);
        ld.setGlobalAddress((long) x);
        return ld;
    }


    @Setup
    public void setup() {
        DataStore dataStore = new DataStore(new HashMap<>(), s -> { });
        StreamLog streamLog = new StreamLogFiles(Paths.get("db", "log"),
                StreamLogDataStore.builder().dataStore(dataStore).build(),
                "100", true);

        for (int i = 0; i < 10000; i++) {

            LogData logData = getEntry(i);
            streamLog.append(i, logData);
        }

    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Fork(1)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 1, time = 30)
    @OperationsPerInvocation(10000)
    public void streamLogInit(Blackhole blackhole) {
        DataStore dataStore = new DataStore(new HashMap<>(), s -> { });
        StreamLog streamLog = new StreamLogFiles(Paths.get("db", "log"),
                StreamLogDataStore.builder().dataStore(dataStore).build(),
                "100", true);
    }




}
