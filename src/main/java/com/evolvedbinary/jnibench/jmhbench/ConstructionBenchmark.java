package com.evolvedbinary.jnibench.jmhbench;

import com.evolvedbinary.jnibench.common.*;
import com.evolvedbinary.jnibench.consbench.NarSystem;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

public class ConstructionBenchmark {
    static {
        NarSystem.loadLibrary();
    }

    private static long check(long ret) {
        if (ret == 0xf00dBeefDeadBeefL) {
            throw new IllegalStateException("Invalid result " + ret);
        }
        return ret;
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"1", "16384"})
        public int batchSize;

        @Param({"32", "1024"})
        public int stringSize;

        @Param({"false"})
        public boolean postProcess;

        public int numStrings = 4096;

        StringProvider stringProvider;

        @Setup(Level.Trial)
        public void setUp() {
            stringProvider = new StringProvider(stringSize, numStrings, postProcess);
            StringProviderStatic.setup(stringProvider);
        }
    }

    @Fork
    @Warmup
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void getStringFromJava(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(thing.getStringFromJava(state.stringProvider, state.batchSize));
        blackhole.consume(state.stringProvider.getInteresting());
    }

    @Fork
    @Warmup
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void getBytesFromJava(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(check(thing.getBytesFromJava(state.stringProvider, state.batchSize)));
        blackhole.consume(state.stringProvider.getInteresting());
    }

    @Fork
    @Warmup
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void getStaticBytesFromJava(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(check(thing.getStaticBytesFromJava(state.batchSize)));
        blackhole.consume(state.stringProvider.getInteresting());
    }

    @Fork
    @Warmup
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void getNativeBytesFromJava(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(check(thing.getStaticNativeFromJava(state.batchSize)));
        blackhole.consume(state.stringProvider.getInteresting());
    }

    @Fork
    @Warmup
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void getNativePreallocatedFromJava(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(check(thing.getStaticNativePreallocatedFromJava(state.batchSize)));
        blackhole.consume(state.stringProvider.getInteresting());
    }

    @Fork
    @Warmup
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void getNativePreallocatedWithSizeFromJava(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(check(thing.getStaticNativePreallocatedWithSizeFromJava(state.batchSize)));
        blackhole.consume(state.stringProvider.getInteresting());
    }

    @Fork
    @Warmup
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void getStaticByteNativeCallerAllocated(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(check(thing.getStaticByteNativeCallerAllocated(state.batchSize)));
        blackhole.consume(state.stringProvider.getInteresting());
    }

    @Fork
    @Warmup
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void getUTF8StringFromJava(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(check(thing.getUTF8StringFromJava(state.stringProvider, state.batchSize)));
        blackhole.consume(state.stringProvider.getInteresting());
    }

    @Fork
    @Warmup
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void getLongByReturnFromJava(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(check(thing.getStaticNativeLongByReturn(state.batchSize)));
        blackhole.consume(state.stringProvider.getInteresting());
    }

    @Fork
    @Warmup
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void getLongByArgFromJava(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(check(thing.getStaticNativeLongByArg(state.batchSize)));
        blackhole.consume(state.stringProvider.getInteresting());
    }

    @Fork
    @Warmup
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void getStringFromJavaAsNativeUTF16(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(thing.getStringFromJavaAsNativeUTF16(state.stringProvider, state.batchSize));
        blackhole.consume(state.stringProvider.getInteresting());
    }
}
