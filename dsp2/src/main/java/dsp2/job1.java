package dsp2;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import resources.StopWords;
import writables.collocation;

public class job1 {

    public static class job1Mapper extends Mapper<LongWritable, Text, collocation, LongWritable> {

        private StopWords stopWords;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            stopWords = StopWords.getInstance();
        }

        @Override
        protected void map(LongWritable lineId, Text line, Context context)
                throws IOException, InterruptedException {

            // Expected input format (Google Ngrams style):
            // "<w1 w2>\t<year>\t<count>\t<...optional...>"
            String[] parts = line.toString().split("\t");
            if (parts.length < 3) return;

            String gram = parts[0].trim();
            String yearStr = parts[1].trim();
            String countStr = parts[2].trim();

            int year;
            try {
                year = Integer.parseInt(yearStr);
            } catch (NumberFormatException e) {
                return;
            }

            long count;
            try {
                count = Long.parseLong(countStr);
            } catch (NumberFormatException e) {
                return;
            }

            int decadeInt = (year / 10) * 10;
            String decade = Integer.toString(decadeInt);

            // Must be exactly 2 words
            String[] wordsRaw = gram.split("\\s+");
            if (wordsRaw.length != 2) return;

            String w1 = normalize(wordsRaw[0]);
            String w2 = normalize(wordsRaw[1]);

            // Like lecturer: only “clean” words survive
            if (!isValidToken(w1) || !isValidToken(w2)) return;

            // Stopwords after normalize
            if (stopWords.isStopWord(w1) || stopWords.isStopWord(w2)) return;

            collocation outKey = new collocation(new Text(decade), new Text(w1), new Text(w2));
            context.write(outKey, new LongWritable(count));
        }
    }

    // ===== Helpers =====

    // ENGLISH ONLY:
    // This removes ALL punctuation/quotes/dashes/etc anywhere in the token,
    // not just at the ends.
    private static String normalize(String w) {
        w = w.toLowerCase(Locale.ROOT);
        w = w.replaceAll("[^a-z]", ""); // keep letters only
        return w;
    }

    private static boolean isValidToken(String w) {
        return w.length() >= 2;
        // If you want exactly like lecturer regex style:
        // return w.length() >= 2 && w.matches("^[a-z]+$");
    }

    // ===== Reducer =====

    public static class job1Reducer extends Reducer<collocation, LongWritable, collocation, LongWritable> {

        @Override
        protected void reduce(collocation key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable v : values) {
                sum += v.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
}
