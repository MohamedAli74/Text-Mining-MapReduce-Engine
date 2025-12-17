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
        // private collocation outputKey = new collocation();
        // private Text star = new Text("*");

        @Override
        protected void map(collocation key ,LongWritable c12 , Context context)
            throws IOException,
            InterruptedException{
                // outputKey.set(key.getDecade(), key.getWord1(), star);
                //DEBUGSystem.out.println("Debugging:Job2Mapper processing key: " + key.toString() + " with value: " + c12.get());
                // context.write(outputKey, c12);// (word1, *)
                context.write(key, c12);// (word1, word2)
            }
            
    }

    public static class UnigramMapper extends Mapper<LongWritable, Text, collocation, LongWritable>{//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        private collocation outputKey = new collocation();
        private LongWritable outputValue = new LongWritable();

        @Override
        protected void map(LongWritable lineId ,Text line , Context context)
            throws IOException,
            InterruptedException{
                //DEBUGSystem.out.println("Debugging:UnigramMapper processing line: " + line.toString());
                //line format - ngram TAB year TAB match_count TAB volume_count NEWLINE
                String[] lineParts = line.toString().split("\t");
                if (lineParts.length < 3) {
                    return;
                }
                String decade = lineParts[1].substring(0,lineParts[1].length()-1)+"0";
                String word = lineParts[0].toString();

                context.getCounter("DecadeCounts", decade).increment(Long.parseLong(lineParts[2]));

                outputKey.set(new Text(decade), new Text(word), new Text("*"));
                outputValue.set(Long.parseLong(lineParts[2]));
                //DEBUGSystem.out.println("Debugging:UnigramMapper emitting: " + outputKey.toString() + " -> " + outputValue.get());
                context.write(outputKey, outputValue);
            }
            
    }

    public static class Job2Partitioner extends Partitioner<collocation, LongWritable>{
        
        @Override
        public int getPartition(collocation key, LongWritable value, int numPartitions) {
            //DEBUGSystem.out.println("Debugging:Job2Partitioner processing key: " + key.toString() + " with value: " + value.get());
            //only hash decade and word1.
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
            //DEBUGSystem.out.println("Debugging:Job2Reducer processing key: " + key.toString() + " with values: " + values.toString());
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
                //DEBUGSystem.out.println("Debugging:Job2Reducer emitting: " + key.toString() + " -> " + outputValue.toString());
                context.write(key, outputValue);
            }
        }
    }
}
