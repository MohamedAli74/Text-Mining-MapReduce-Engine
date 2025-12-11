package dsp2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import writables.collocation;

public class job3 {
    public static class job3Mapper extends Mapper<collocation, Text, collocation, Text>{//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        private collocation outputKey = new collocation();
        private Text star = new Text("*");
        //input: (decade w1 w2), "c12,c1"
        @Override
        protected void map(collocation key ,Text value , Context context)
            throws IOException,
            InterruptedException{
                outputKey.set(key.getDecade(), key.getWord2(), star);

                context.write(outputKey, new Text(value.toString().split(",")[0]));// (word2, *) "c12"
                key.set(key.getDecade(),key.getWord2(),key.getWord2());
                context.write(key, value);// (word2, word1) "c12,c1"
            }
            
    }


    public static class Job3Partitioner extends Partitioner<collocation, Text>{
        
        @Override
        public int getPartition(collocation key, Text value, int numPartitions) {
            //only hash 'decade' and 'word1'.
            int hash = key.getDecade().hashCode() + key.getWord1().hashCode();
            return (hash & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class job3Reducer extends Reducer<collocation, Text, collocation, Text>{
        private Text outputValue = new Text();
        // Variables to hold state between iterations
        private long currentC2 = 0;
        private String currentDecade = "";
        private String currentWord2 = "";

        //input: (decade * word2) "c12" or (decade word1 word2) "c12,c1"

        @Override
        protected void reduce(collocation key, Iterable<Text> values, Context context)
            throws IOException,
            InterruptedException{
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
                    String valStr = val.toString();
                    
                    // Output Value: "c12,c1,c2"
                    outputValue.set(valStr + "," + currentC2);
                    
                    //Swap the key back for display
                    collocation finalKey = new collocation();
                    finalKey.set(new Text(decade), key.getWord2(), key.getWord1());
                    
                    context.write(finalKey, outputValue);
                }
            }
            //output: (decade w1 w2), "c12,c1,c2"
        }
    }
    
}
