package galiglobal.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public  class PrintSinkFunction implements SinkFunction<Tuple2<String, Integer>> {
    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) {
        if (value.f1 > 2) {
//            System.out.println("Word: " + value.f0 + ", Count: " + value.f1);
        }
    }
}