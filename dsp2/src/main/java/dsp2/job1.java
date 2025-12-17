package dsp2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import resources.StopWords;
import writables.collocation;


public class job1 {
    

    public static class job1Mapper extends Mapper<LongWritable, Text, collocation, LongWritable>{//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        StopWords stopsWords;
        
        @Override
        protected void setup(Context context)
        throws IOException,
        InterruptedException{
            stopsWords = StopWords.getInstance();
        }
        
        @Override
        protected void map(LongWritable lineId ,Text line , Context context)
            throws IOException,
            InterruptedException{
                //DEBUGSystem.out.println("Debugging:Job1Mapper processing line: " + line.toString());
                //line format - ngram TAB year TAB match_count TAB volume_count NEWLINE
                String[] lineParts = line.toString().split("\t");
                if (lineParts.length < 3) {
                    return;
                }
                String decade = lineParts[1].substring(0,lineParts[1].length()-1)+"0";
                String[] words = lineParts[0].toString().split(" ");
                if (words.length != 2) {
                    return;
                }
                if(stopsWords.isStopWord(words[0]) | stopsWords.isStopWord(words[1]))return;
                collocation key = new collocation(new Text(decade) ,new Text(words[0]), new Text(words[1]));
                LongWritable value = new LongWritable(Long.parseLong(lineParts[2]));
                //DEBUGSystem.out.println("Debugging:Job1Mapper emitting: " + key.toString() + " -> " + value.get());
                context.write(key,value);
            }
            
    }
    
    

    public static class job1Reducer extends Reducer<collocation, LongWritable, collocation, LongWritable>{//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        @Override
        public void reduce(collocation collocation, Iterable<LongWritable> counts, Context context)
            throws IOException,
            InterruptedException{
                //DEBUGSystem.out.println("Debugging:Job1Reducer processing key: " + collocation.toString()+ " with values:" + counts.toString());
                long sum = 0;
                for(LongWritable count : counts)
                    sum += count.get();
                //DEBUGSystem.out.println("Debugging:Job1Reducer emitting: " + collocation.toString() + " -> " + sum);
                context.write(collocation, new LongWritable(sum));
                //context.getCounter("DecadeCounts", collocation.getDecade().toString()).increment(sum);
            }
    }

}
