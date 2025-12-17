package dsp2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import writables.DoublePair;
import writables.collocation;

public class job4 {
    /**
     * @input key -> (decade, word1, word2), value -> "Rario"
     * @output key -> (decade,ratio), value -> "word1,word2"
     */
    public static class job4Mapper extends Mapper<collocation, DoubleWritable, DoublePair, Text>{
        @Override
        protected void map(collocation key ,DoubleWritable ratio , Context context)
            throws IOException,
            InterruptedException {
                //DEBUGSystem.out.println("Debugging:Job4Mapper processing key: " + key.toString() + " with value: " + ratio.get());
                DoublePair outKey = new DoublePair(key.getDecade(),ratio);
                Text outValue = new Text(key.getWord1().toString() +","+ key.getWord2().toString());
                //DEBUGSystem.out.println("Debugging:Job4Mapper emitting: " + outKey.toString() + " -> " + outValue.toString());
                context.write(outKey, outValue); 
        }
    }

    public static class Job4Partitioner extends Partitioner<DoublePair, Text>{
        
        @Override
        public int getPartition(DoublePair key, Text value, int numPartitions) {
            //DEBUGSystem.out.println("Debugging:Job4Partitioner processing key: " + key.toString() + " with value: " + value.toString());
            //only hash 'decade' to ensure all the collocations from the same decade gets to the same reducer.
            int hash = key.getDecade().toString().hashCode();
            return (hash & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class DecadeGroupingComparator extends WritableComparator {//TODO: use this in the main
        
        // 1. Constructor: Tells Hadoop to convert byte streams into 'DoublePair' objects
        //    so we can compare them easily.
        protected DecadeGroupingComparator() {
            super(DoublePair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DoublePair p1 = (DoublePair) a;
            DoublePair p2 = (DoublePair) b;
            
            //Compare ONLY the decade. Ignore the ratio
            return p1.getDecade().compareTo(p2.getDecade());
        }
    }

    public static class Job4Reducer extends Reducer<DoublePair, Text, DoublePair, Text>{        
        @Override
        // Running EXACTLY ONCE per decade
        public void reduce(DoublePair key, Iterable<Text> values, Context context)
            throws IOException,
            InterruptedException {
            //DEBUGSystem.out.println("Debugging:Job4Reducer processing key: " + key.toString() + " with values: " + values.toString());
            int counter = 0;

            for (Text val : values) {
                if (counter < 100) {
                    //DEBUGSystem.out.println("Debugging:Job4Reducer emitting: " + key.toString() + " -> " + val.toString());
                    context.write(key, val);
                    counter++;
                } else {
                    return; 
                }
            }
        }
    }
}
