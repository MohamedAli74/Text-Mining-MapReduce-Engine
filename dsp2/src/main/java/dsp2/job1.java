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

    /**
     * Cleans a token so it becomes ONLY letters (English or Hebrew),
     * after removing punctuation from start/end.
     * Returns null if invalid.
     */
    private static String cleanToken(String raw) {
        if (raw == null) return null;

        String w = raw.trim().toLowerCase(Locale.ROOT);
        if (w.isEmpty()) return null;

        // Remove non-letter chars from the beginning/end
        // Keep: a-z and Hebrew block \u0590-\u05FF
        w = w.replaceAll("^[^a-z\\u0590-\\u05FF]+", "");
        w = w.replaceAll("[^a-z\\u0590-\\u05FF]+$", "");

        if (w.length() < 2) return null;

        // Must be letters only after cleaning
        if (!w.matches("^[a-z\\u0590-\\u05FF]+$")) return null;

        return w;
    }

    public static class job1Mapper extends Mapper<LongWritable, Text, collocation, LongWritable> {

        private StopWords stopWords;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            stopWords = StopWords.getInstance();
        }

        @Override
        protected void map(LongWritable lineId, Text line, Context context)
                throws IOException, InterruptedException {

            // Expected format (Google Ngrams text):
            // <ngram>\t<year>\t<count>\t<...optional...>
            String[] parts = line.toString().split("\t");
            if (parts.length < 3) return;

            // year -> decade
            int year;
            try {
                year = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException e) {
                return;
            }
            int decadeInt = (year / 10) * 10;
            String decade = Integer.toString(decadeInt);

            // 2gram must be exactly 2 tokens
            String[] wordsRaw = parts[0].trim().split("\\s+");
            if (wordsRaw.length != 2) return;

            String w1 = cleanToken(wordsRaw[0]);
            String w2 = cleanToken(wordsRaw[1]);
            if (w1 == null || w2 == null) return;

            // StopWords check should be case-insensitive:
            // we already lowercase in cleanToken, but do it anyway for safety.
            if (stopWords.isStopWord(w1.toLowerCase(Locale.ROOT)) ||
                stopWords.isStopWord(w2.toLowerCase(Locale.ROOT))) {
                return;
            }

            long count;
            try {
                count = Long.parseLong(parts[2].trim());
            } catch (NumberFormatException e) {
                return;
            }

            collocation outKey = new collocation(new Text(decade), new Text(w1), new Text(w2));
            context.write(outKey, new LongWritable(count));
        }
    }

    public static class job1Reducer extends Reducer<collocation, LongWritable, collocation, LongWritable> {

        @Override
        public void reduce(collocation key, Iterable<LongWritable> counts, Context context)
                throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable c : counts) {
                sum += c.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
}
