package galiglobal.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class RandomWordCount {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // Generate a stream of random words
        DataStream<String> randomWords = env.addSource(new RandomWordSource());

        // Split the words, group by the words, and sum the occurrences
        DataStream<Tuple2<String, Integer>> wordCounts = randomWords
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1);

        // Use a custom sink function to print the results
        wordCounts.addSink(new PrintSinkFunction());

        // Execute the Flink job
        env.execute("Random Word Count Example");
    }

    // Source function to generate random words


    // User-defined function to split lines into words
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Emit the pairs (word, 1)
            out.collect(new Tuple2<>(value, 1));
        }
    }

    // Custom sink function to print the results
    public static class PrintSinkFunction implements SinkFunction<Tuple2<String, Integer>> {
        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) {
            if (value.f1 > 2) {
                System.out.println("Word: " + value.f0 + ", Count: " + value.f1);
            }
        }
    }
}