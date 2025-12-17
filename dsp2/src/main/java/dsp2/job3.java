package dsp2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import writables.collocation;

public class job3 {
    public static class job3Mapper extends Mapper<collocation, Text, collocation, Text>{//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        //input: (decade w1 w2), "c12,c1"
        @Override
        protected void map(collocation key ,Text value , Context context)
            throws IOException,
            InterruptedException{
                //DEBUGSystem.out.println("Debugging:Job3Mapper processing key: " + key.toString() + " with value: " + value.toString());
                key.set(key.getDecade(),key.getWord2(),key.getWord1());
                //DEBUGSystem.out.println("Debugging:Job3Mapper emitting: " + key.toString() + " -> " + value.toString());
                context.write(key, value);// (word2, word1) "c12,c1"
            }
            
    }

    public static class UnigramMapper extends Mapper<LongWritable, Text, collocation, Text> {

        private collocation outputKey = new collocation();
        private Text outputValue = new Text();
        private Text star = new Text("*");

        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            //DEBUGSystem.out.println("Debugging:UnigramMapper processing line: " + value.toString());
            
            // Input Format: "word <TAB> year <TAB> match_count <TAB> volume_count"
            String[] parts = value.toString().split("\t");

            if (parts.length < 3) return;

            String word = parts[0];
            String yearStr = parts[1];
            String countStr = parts[2]; // This is the c2

            int year = Integer.parseInt(yearStr);
            int decadeInt = (year / 10) * 10;
            String decade = String.valueOf(decadeInt);

            // (Decade, Word, "*")
            outputKey.set(new Text(decade), new Text(word), star);
            outputValue.set(countStr);
            //DEBUGSystem.out.println("Debugging:UnigramMapper emitting: " + outputKey.toString() + " -> " + outputValue.toString());
            context.write(outputKey, outputValue);
        }
    }

    public static class Job3Partitioner extends Partitioner<collocation, Text>{
        
        @Override
        public int getPartition(collocation key, Text value, int numPartitions) {
            //DEBUGSystem.out.println("Debugging:Job3Partitioner processing key: " + key.toString() + " with value: " + value.toString());
            //only hash 'decade' and 'word1'.
            int hash = key.getDecade().hashCode() + key.getWord1().hashCode();
            return (hash & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class job3Reducer extends Reducer<collocation, Text, collocation, DoubleWritable>{
        // Variables to hold state between iterations
        private long currentC2 = 0;
        private String currentDecade = "";
        private String currentWord2 = "";

        //input: (decade word2 *) "c2" or (decade word2 word1) "c12,c1"

        @Override
        protected void reduce(collocation key, Iterable<Text> values, Context context)
            throws IOException,
            InterruptedException{
            //DEBUGSystem.out.println("Debugging:Job3Reducer processing key: " + key.toString() + " with values: " + values.toString());
            String decade = key.getDecade().toString();
            String w2 = key.getWord1().toString();
            
            // If this is a new group, reset the counter
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
            }
            else{
                for (Text val : values) {
                    // Value is "c12,c1"
                    String[] valParts = val.toString().split(",");
                    long c12 = Long.parseLong(valParts[0]);
                    long c1 = Long.parseLong(valParts[1]);
                    long N = context.getConfiguration().getLong("N_" + decade, -1);
                    if (N == -1) {
                        throw new RuntimeException("Error: Missing N for decade " + decade);
                    }
                    double value = likelihoodRatio.get(c1, currentC2, c12, N);
                    
                    //Swap the key back for display
                    collocation finalKey = new collocation();
                    finalKey.set(new Text(decade), key.getWord2(), key.getWord1());
                    //DEBUGSystem.out.println("Debugging:Job3Reducer emitting: " + finalKey.toString() + " -> " + value);
                    context.write(finalKey, new DoubleWritable(value));
                }
            }
            //output: (decade w1 w2), raio(55.2)
        }
    }
    
}
