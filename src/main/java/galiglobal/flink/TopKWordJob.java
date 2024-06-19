package galiglobal.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;

public class TopKWordJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // Generate a stream of random words
        DataStream<String> randomWords = env.addSource(new RandomWordSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
                );

        // Split the words, group by the words, and sum the occurrences
        DataStream<Tuple2<String, Integer>> wordCounts = randomWords
                .flatMap(new WordTokenizer())
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .sum(1);

        // Use a custom sink function to print the results
        wordCounts.addSink(new PrintSinkFunction());

        // Find the top 3 words every minute
        DataStream<String> topWords = wordCounts
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new AllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> values, Collector<String> out) {
                        PriorityQueue<Tuple2<String, Integer>> topWordsQueue = new PriorityQueue<>(Comparator.comparingInt(o -> o.f1));
                        for (Tuple2<String, Integer> wordCount : values) {
                            topWordsQueue.add(wordCount);
                            if (topWordsQueue.size() > 3) {
                                topWordsQueue.poll();
                            }
                        }
                        StringBuilder result = new StringBuilder("Top 3 words: ");
                        while (!topWordsQueue.isEmpty()) {
                            Tuple2<String, Integer> wordCount = topWordsQueue.poll();
                            result.append(wordCount.f0).append(" (").append(wordCount.f1).append("), ");
                        }
                        out.collect(result.toString());
                    }
                });

        // Print the results
        topWords.print();

        // Execute the Flink job
        env.execute("Random Word Count Example");
    }


}
