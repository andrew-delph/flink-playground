package adelph.basic;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

public  class RandomWordSource extends RichSourceFunction<String> {
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";
    private static final int WORD_LENGTH = 3;
    private final Random random = new Random();
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws InterruptedException {
        while (isRunning) {
            StringBuilder word = new StringBuilder(WORD_LENGTH);
            for (int i = 0; i < WORD_LENGTH; i++) {
                int randomIndex = random.nextInt(ALPHABET.length());
                word.append(ALPHABET.charAt(randomIndex));
            }
            ctx.collect(word.toString());
//            Thread.sleep(20); // Sleep for a short time to simulate data generation
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
