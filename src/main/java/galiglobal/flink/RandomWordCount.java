package galiglobal.flink;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RandomWordCount {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // Generate a stream of random words
        DataStream<String> randomWords = env.addSource(new RandomWordSource());

        // Split the words, group by the words, and sum the occurrences
        DataStream<Tuple2<String, Integer>> wordCounts = randomWords
                .flatMap(new WordTokenizer())
                .keyBy(value -> value.f0)
                .sum(1);

        // Use a custom sink function to print the results
        wordCounts.addSink(new PrintSinkFunction());

        // Execute the Flink job
        env.execute("Random Word Count Example");
    }


}