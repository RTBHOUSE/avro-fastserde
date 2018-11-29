package com.rtbhouse.utils.avro.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import com.rtbhouse.utils.generated.avro.benchmark.LargeAndDeep;

@State(Scope.Benchmark)
@Fork(1)
public class LargeAndDeepRecordBenchmark extends RecordBenchmarkBase<LargeAndDeep> {

    @Override
    public void init() throws Exception {
        specificRecordSchema = LargeAndDeep.getClassSchema();
        super.init();
    }

    @Benchmark
    public void fastGenericDatumReader() throws Exception {
        super.fastGenericDatumReader();
    }

    @Benchmark
    public void genericDatumReader() throws Exception {
        super.genericDatumReader();
    }

    @Benchmark
    public void fastGenericDatumWriter() throws Exception {
        super.fastGenericDatumWriter();
    }

    @Benchmark
    public void genericDatumWriter() throws Exception {
        super.genericDatumWriter();
    }

    @Benchmark
    public void fastSpecificDatumReader() throws Exception {
        super.fastSpecificDatumReader();
    }

    @Benchmark
    public void specificDatumReader() throws Exception {
        super.specificDatumReader();
    }

    @Benchmark
    public void fastSpecificDatumWriter() throws Exception {
        super.fastSpecificDatumWriter();
    }

    @Benchmark
    public void specificDatumWriter() throws Exception {
        super.specificDatumWriter();
    }

}
