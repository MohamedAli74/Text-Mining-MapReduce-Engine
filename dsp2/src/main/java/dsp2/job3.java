package dsp2;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import writables.collocation;

public class job3 {

    private static String cleanToken(String raw) {
        if (raw == null) return null;

        String w = raw.trim().toLowerCase(Locale.ROOT);

        w = w.replaceAll("^[^a-z\\u0590-\\u05FF]+", "");
        w = w.replaceAll("[^a-z\\u0590-\\u05FF]+$", "");

        if (w.length() < 2) return null;
        if (!w.matches("^[a-z\\u0590-\\u05FF]+$")) return null;

        return w;
    }

    public static class job3Mapper extends Mapper<collocation, Text, collocation, Text> {
        // input: (decade w1 w2), "c12,c1"
        @Override
        protected void map(collocation key, Text value, Context context)
                throws IOException, InterruptedException {

            // swap w1<->w2 for grouping by (decade, w2)
            key.set(key.getDecade(), key.getWord2(), key.getWord1());
            context.write(key, value); // (decade, word2, word1) "c12,c1"
        }
    }

    public static class UnigramMapper extends Mapper<LongWritable, Text, collocation, Text> {

        private collocation outputKey = new collocation();
        private Text outputValue = new Text();
        private final Text star = new Text("*");

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input: "word<TAB>year<TAB>match_count<TAB>volume_count"
            String[] parts = value.toString().split("\t");
            if (parts.length < 3) return;

            String word = cleanToken(parts[0]);
            if (word == null) return;

            int year;
            try {
                year = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException e) {
                return;
            }
            int decadeInt = (year / 10) * 10;
            String decade = String.valueOf(decadeInt);

            // c2
            String countStr = parts[2].trim();

            // (Decade, Word, "*") -> "c2"
            outputKey.set(new Text(decade), new Text(word), star);
            outputValue.set(countStr);
            context.write(outputKey, outputValue);
        }
    }

    public static class Job3Partitioner extends Partitioner<collocation, Text> {
        @Override
        public int getPartition(collocation key, Text value, int numPartitions) {
            int hash = key.getDecade().hashCode() + key.getWord1().hashCode();
            return (hash & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class job3Reducer extends Reducer<collocation, Text, collocation, DoubleWritable> {

        private long currentC2 = 0;
        private String currentDecade = "";
        private String currentWord2 = "";

        // input: (decade word2 *) "c2" or (decade word2 word1) "c12,c1"
        @Override
        protected void reduce(collocation key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String decade = key.getDecade().toString();
            String w2 = key.getWord1().toString();

            if (!decade.equals(currentDecade) || !w2.equals(currentWord2)) {
                currentDecade = decade;
                currentWord2 = w2;
                currentC2 = 0;
            }

            if (key.getWord2().toString().equals("*")) {
                long sum = 0;
                for (Text val : values) {
                    sum += Long.parseLong(val.toString());
                }
                currentC2 = sum;
            } else {
                for (Text val : values) {
                    String[] valParts = val.toString().split(",");
                    long c12 = Long.parseLong(valParts[0]);
                    long c1  = Long.parseLong(valParts[1]);

                    long N = context.getConfiguration().getLong("N_" + decade, -1);
                    if (N == -1) {
                        throw new RuntimeException("Error: Missing N for decade " + decade);
                    }

                    double score = likelihoodRatio.get(c1, currentC2, c12, N);

                    // swap key back for output display: (decade w1 w2)
                    collocation finalKey = new collocation();
                    finalKey.set(new Text(decade), key.getWord2(), key.getWord1());
                    context.write(finalKey, new DoubleWritable(score));
                }
            }
        }
    }
}
