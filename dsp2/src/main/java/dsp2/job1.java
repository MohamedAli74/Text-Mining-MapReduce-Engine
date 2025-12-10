package main.java.dsp2;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWriteable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import com.yourname.collocation.writables.DecadeKey;

import writables.collocation;
import resources.stopsWords;


public class job1 {
    

    public static class job1Mapper extends Mapper<LongWriteable, Text, collocation, LongWriteable>{//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        StopWords stopsWords;
        
        @Override
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException,
        InterruptedException{
            stopsWords = StopWords.getInstance();
        }
        
        @Override
        protected void map(LongWriteable lineId ,Text line , org.apache.hadoop.mapreduce.Mapper.Context context)
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
                if(stopsWords.isStopWord(word[0]) | stopsWords.isStopWord(word[1]))return;
                collocation key = new collocation(new Text(decade) ,new Text(words[0]), new Text(words[1]));
                // collocation key2 = new collocation(new Text(decade) ,new Text(words[0]), new Text("*"));
                // collocation key3 = new collocation(new Text(decade) ,new Text(words[1]), new Text("*"));
                LongWriteable value = new LongWriteable(Long.parseLong(LineParts[2]));
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
    
    

    public static class job1Reducer extends Reducer<collocation, LongWriteable, collocation, LongWriteable>{//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        @Override
        void reduce(collocation collocation, Iterable<LongWriteable> counts, org.apache.hadoop.mapreduce.Reducer.Context context)
            throws IOException{
                long sum = 0;
                for(LongWriteable count : counts)
                    sum += count.get();
                context.write(collocation, new LongWriteable(sum));
                context.getCounter("DecadeCounts", key.getDecade().toString()).increment(sum);
            }
    }

}
