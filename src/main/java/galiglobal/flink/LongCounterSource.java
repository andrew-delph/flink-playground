package galiglobal.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class LongCounterSource extends RichParallelSourceFunction<Long> {

    private volatile boolean cancelled = false;
    private Random random;
    private long counter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        random = new Random();
//        counter = new Long();
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (!cancelled) {
            Long nextLong = random.nextLong();
            counter++;
            if( counter > 10){
                counter = 0;
            }
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(counter);
            }
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}