package dsp2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import writables.collocation;
import resources.StopWords;


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
                // collocation key2 = new collocation(new Text(decade) ,new Text(words[0]), new Text("*"));
                // collocation key3 = new collocation(new Text(decade) ,new Text(words[1]), new Text("*"));
                LongWritable value = new LongWritable(Long.parseLong(lineParts[2]));
                context.write(key,value);
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
    
    

    public static class job1Reducer extends Reducer<collocation, LongWritable, collocation, LongWritable>{//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        @Override
        public void reduce(collocation collocation, Iterable<LongWritable> counts, Context context)
            throws IOException,
            InterruptedException{
                long sum = 0;
                for(LongWritable count : counts)
                    sum += count.get();
                context.write(collocation, new LongWritable(sum));
                context.getCounter("DecadeCounts", collocation.getDecade().toString()).increment(sum);
            }
    }

}
