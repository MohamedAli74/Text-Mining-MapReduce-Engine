package dsp2;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

import writables.collocation;

public class job2 {

    private static String cleanToken(String raw) {
        if (raw == null) return null;

        String w = raw.trim().toLowerCase(Locale.ROOT);

        w = w.replaceAll("^[^a-z\\u0590-\\u05FF]+", "");
        w = w.replaceAll("[^a-z\\u0590-\\u05FF]+$", "");

        if (w.length() < 2) return null;
        if (!w.matches("^[a-z\\u0590-\\u05FF]+$")) return null;

        return w;
    }

    public static class job2Mapper extends Mapper<collocation, LongWritable, collocation, LongWritable> {

        @Override
        protected void map(collocation key, LongWritable c12, Context context)
                throws IOException, InterruptedException {
            context.write(key, c12); // (word1, word2)
        }
    }

    public static class UnigramMapper extends Mapper<LongWritable, Text, collocation, LongWritable> {

        private collocation outputKey = new collocation();
        private LongWritable outputValue = new LongWritable();

        @Override
        protected void map(LongWritable lineId, Text line, Context context)
                throws IOException, InterruptedException {

            String[] lineParts = line.toString().split("\t");
            if (lineParts.length < 3) return;

            String decade = lineParts[1].substring(0, lineParts[1].length() - 1) + "0";

            String word = cleanToken(lineParts[0]);
            if (word == null) return;

            long c1;
            try {
                c1 = Long.parseLong(lineParts[2]);
            } catch (NumberFormatException e) {
                return;
            }

            context.getCounter("DecadeCounts", decade).increment(c1);

            outputKey.set(new Text(decade), new Text(word), new Text("*"));
            outputValue.set(c1);
            context.write(outputKey, outputValue);
        }
    }

    public static class Job2Partitioner extends Partitioner<collocation, LongWritable> {
        @Override
        public int getPartition(collocation key, LongWritable value, int numPartitions) {
            int hash = key.getDecade().hashCode() + key.getWord1().hashCode();
            return (hash & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class job2Reducer extends Reducer<collocation, LongWritable, collocation, Text> {
        private Text outputValue = new Text();

        private long currentC1 = 0;
        private String currentDecade = "";
        private String currentWord1 = "";

        @Override
        protected void reduce(collocation key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            String decade = key.getDecade().toString();
            String w1 = key.getWord1().toString();

            if (!decade.equals(currentDecade) || !w1.equals(currentWord1)) {
                currentDecade = decade;
                currentWord1 = w1;
                currentC1 = 0;
            }

            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }

            if (key.getWord2().toString().equals("*")) {
                currentC1 = sum;
            } else {
                outputValue.set(sum + "," + currentC1);
                context.write(key, outputValue);
            }
        }
    }
}
