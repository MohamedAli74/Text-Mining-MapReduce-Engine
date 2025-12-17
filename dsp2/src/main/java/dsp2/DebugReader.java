package dsp2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import writables.collocation; 

public class DebugReader {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        collocation key = new collocation();

        // --------------------------------------------------
        // READ JOB 1 OUTPUT (LongWritable)
        // --------------------------------------------------
        System.out.println("\n--- READING JOB 1 OUTPUT (Bigram Counts) ---");
        try {
            Path p1 = new Path("output_local/job1_out/part-r-00000");
            SequenceFile.Reader r1 = new SequenceFile.Reader(conf, SequenceFile.Reader.file(p1));
            LongWritable val1 = new LongWritable(); // <--- MUST BE LONG
            
            while (r1.next(key, val1)) {
                System.out.println(key.toString() + " \tCount: " + val1.get());
            }
            r1.close();
        } catch (Exception e) { System.out.println("Job 1 Read Error: " + e.getMessage()); }

        // --------------------------------------------------
        // READ JOB 2 OUTPUT (Text)
        // --------------------------------------------------
        System.out.println("\n--- READING JOB 2 OUTPUT (c12, c1) ---");
        try {
            Path p2 = new Path("output_local/job2_out/part-r-00000");
            SequenceFile.Reader r2 = new SequenceFile.Reader(conf, SequenceFile.Reader.file(p2));
            Text val2 = new Text(); // <--- MUST BE TEXT
            
            while (r2.next(key, val2)) {
                System.out.println(key.toString() + " \tVal: " + val2.toString());
            }
            r2.close();
        } catch (Exception e) { System.out.println("Job 2 Read Error: " + e.getMessage()); }

        // --------------------------------------------------
        // READ JOB 3 OUTPUT (DoubleWritable)
        // --------------------------------------------------
        System.out.println("\n--- READING JOB 3 OUTPUT (Scores) ---");
        try {
            Path p3 = new Path("output_local/job3_out/part-r-00000");
            SequenceFile.Reader r3 = new SequenceFile.Reader(conf, SequenceFile.Reader.file(p3));
            DoubleWritable val3 = new DoubleWritable(); // <--- MUST BE DOUBLE
            
            while (r3.next(key, val3)) {
                System.out.println(key.toString() + " \tScore: " + val3.get());
            }
            r3.close();
        } catch (Exception e) { System.out.println("Job 3 Read Error: " + e.getMessage()); }
    }
}