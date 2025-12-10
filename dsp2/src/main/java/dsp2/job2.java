package dsp2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

import writables.collocation;

public class job2 {
    public static class job2Mapper extends Mapper<collocation, LongWritable, collocation, LongWritable>{//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        private collocation outputKey = new collocation();
        private Text star = new Text("*");

        // @Override
        // protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        // throws IOException,
        // InterruptedException{}
        
        @Override
        protected void map(collocation key ,LongWritable c12 , Context context)
            throws IOException,
            InterruptedException{
                outputKey.set(key.getDecade(), key.getWord1(), star);

                context.write(outputKey, c12);// (word1, *)
                context.write(key, c12);// (word1, word2)
            }
            
        // @Override
        // protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
        // throws IOException,
        // InterruptedException{}
        
        // @Override
        // public void run(org.apache.hadoop.mapreduce.Mapper.Context context)
        // throws IOException,
        // InterruptedException{}
    }


    public static class Job2Partitioner extends Partitioner<collocation, LongWritable>{
        
        @Override
        public int getPartition(collocation key, LongWritable value, int numPartitions) {
            //only hash 'decade' and 'word1'.
            int hash = key.getDecade().hashCode() + key.getWord1().hashCode();
            return (hash & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class job2Reducer extends Reducer<collocation, LongWritable, collocation, Text>{
        private Text outputValue = new Text();
        // Variables to hold state between iterations
        private long currentC1 = 0;
        private String currentDecade = "";
        private String currentWord1 = "";

        @Override
        protected void reduce(collocation key, Iterable<LongWritable> values, Context context)
            throws IOException,
            InterruptedException{
            String decade = key.getDecade().toString();
            String w1 = key.getWord1().toString();
            
            // If this is a new group, reset the counter
            if (!decade.equals(currentDecade) || !w1.equals(currentWord1)) {
                currentDecade = decade;
                currentWord1 = w1;
                currentC1 = 0;
            }
            long sum = 0;
            for(LongWritable value : values){
                sum+=value.get();
            }
            if (key.getWord2().toString().equals("*")) {
                currentC1 = sum;
            }else{
                outputValue.set(sum + "," + currentC1);
                context.write(key, outputValue);
            }
        }
    }
}
