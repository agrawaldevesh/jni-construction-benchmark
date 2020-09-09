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

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"16384"})
        public int batchSize;

        @Param({"32", "1024"})
        public int stringSize;

        @Param({"false"})
        public boolean postProcess;

        public int numStrings = 256;

        StringProvider stringProvider;

        @Setup(Level.Trial)
        public void setUp() {
            stringProvider = new StringProvider(stringSize, numStrings, postProcess);
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
    public void getStringFromJavaNoWork(Blackhole blackhole, BenchmarkState state) {
        FooByCallStaticFinal thing = new FooByCallStaticFinal(false);
        blackhole.consume(thing.getStringFromJavaNoWork(state.stringProvider, state.batchSize));
        blackhole.consume(state.stringProvider.getInteresting());
    }
}
