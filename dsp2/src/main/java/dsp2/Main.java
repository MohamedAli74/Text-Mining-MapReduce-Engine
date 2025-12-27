package dsp2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import writables.DoublePair;
import writables.collocation;

public class Main {

    private static void deleteIfExists(Configuration conf, Path p) throws Exception {
        FileSystem fs = p.getFileSystem(conf);
        if (fs.exists(p)) {
            fs.delete(p, true);
        }
    }

    private static void loadDecadeCountersIntoConf(Job finishedJob2, Configuration conf) throws Exception {
        Counters counters = finishedJob2.getCounters();
        CounterGroup g = counters.getGroup("DecadeCounts");
        for (Counter c : g) {
            String decade = c.getName();
            long N = c.getValue();
            conf.setLong("N_" + decade, N);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: dsp2.Main <unigram_input> <bigram_input> <output_base> <reducers> [useCombiner(0/1)]");
            System.exit(1);
        }

        String unigramInput = args[0];
        String bigramInput  = args[1];
        String outBase      = args[2];
        int reducers        = Integer.parseInt(args[3]);

        boolean useCombiner = (args.length >= 5) && "1".equals(args[4]);

        Configuration conf = new Configuration();

        Path out1 = new Path(outBase + "/job1_out");
        Path out2 = new Path(outBase + "/job2_out");
        Path out3 = new Path(outBase + "/job3_out");
        Path out4 = new Path(outBase + "/job4_out");

        deleteIfExists(conf, out1);
        deleteIfExists(conf, out2);
        deleteIfExists(conf, out3);
        deleteIfExists(conf, out4);

        System.out.println("Reducers: " + reducers);
        System.out.println("useCombiner(Job1): " + useCombiner);

        // -------------------------
        // Job 1 (BIGRAMS DATASET) 
        // -------------------------
        Job j1 = Job.getInstance(conf, "job1 - build (decade,w1,w2)->c12");
        j1.setJarByClass(Main.class);

        j1.setInputFormatClass(SequenceFileInputFormat.class);

        j1.setMapperClass(job1.job1Mapper.class);
        j1.setReducerClass(job1.job1Reducer.class);

        if (useCombiner) {
            j1.setCombinerClass(job1.job1Reducer.class);
        }
        j1.setMapOutputKeyClass(collocation.class);
        j1.setMapOutputValueClass(LongWritable.class);

        j1.setOutputKeyClass(collocation.class);
        j1.setOutputValueClass(LongWritable.class);
        j1.setOutputFormatClass(SequenceFileOutputFormat.class);
        j1.setNumReduceTasks(reducers);

        FileInputFormat.addInputPath(j1, new Path(bigramInput));
        FileOutputFormat.setOutputPath(j1, out1);

        if (!j1.waitForCompletion(true)) {
            System.err.println("Job1 failed");
            System.exit(2);
        }

        // -------------------------
        // Job 2 (JOIN c1 from UNIGRAMS) <-- SequenceFileInputFormat
        // -------------------------
        Job j2 = Job.getInstance(conf, "job2 - Join c1 from Unigrams");
        j2.setJarByClass(Main.class);

        MultipleInputs.addInputPath(
                j2, out1,
                SequenceFileInputFormat.class,
                job2.job2Mapper.class
        );

        MultipleInputs.addInputPath(
                j2, new Path(unigramInput),
                SequenceFileInputFormat.class,
                job2.UnigramMapper.class
        );

        j2.setPartitionerClass(job2.Job2Partitioner.class);
        j2.setReducerClass(job2.job2Reducer.class);

        j2.setMapOutputKeyClass(collocation.class);
        j2.setMapOutputValueClass(LongWritable.class);

        j2.setOutputKeyClass(collocation.class);
        j2.setOutputValueClass(Text.class);

        j2.setOutputFormatClass(SequenceFileOutputFormat.class);
        j2.setNumReduceTasks(reducers);

        FileOutputFormat.setOutputPath(j2, out2);

        if (!j2.waitForCompletion(true)) {
            System.err.println("Job2 failed");
            System.exit(3);
        }

        loadDecadeCountersIntoConf(j2, conf);

        // -------------------------
        // Job 3 (JOIN c2 from UNIGRAMS) <-- SequenceFileInputFormat
        // -------------------------
        Job j3 = Job.getInstance(conf, "job3 - Join c2 from Unigrams");
        j3.setJarByClass(Main.class);

        MultipleInputs.addInputPath(
                j3, out2,
                SequenceFileInputFormat.class,
                job3.job3Mapper.class
        );

        MultipleInputs.addInputPath(
                j3, new Path(unigramInput),
                SequenceFileInputFormat.class,
                job3.UnigramMapper.class
        );

        j3.setPartitionerClass(job3.Job3Partitioner.class);
        j3.setReducerClass(job3.job3Reducer.class);

        j3.setMapOutputKeyClass(collocation.class);
        j3.setMapOutputValueClass(Text.class);

        j3.setOutputKeyClass(collocation.class);
        j3.setOutputValueClass(DoubleWritable.class);

        j3.setOutputFormatClass(SequenceFileOutputFormat.class);
        j3.setNumReduceTasks(reducers);

        FileOutputFormat.setOutputPath(j3, out3);

        if (!j3.waitForCompletion(true)) {
            System.err.println("Job3 failed");
            System.exit(4);
        }

        // -------------------------
        // Job 4 (TOP100 per decade) -> FINAL TEXT OUTPUT
        // -------------------------
        Job j4 = Job.getInstance(conf, "job4 - top100 per decade");
        j4.setJarByClass(Main.class);

        // input is SequenceFile from job3
        j4.setInputFormatClass(SequenceFileInputFormat.class);

        j4.setMapperClass(job4.job4Mapper.class);
        j4.setPartitionerClass(job4.Job4Partitioner.class);
        j4.setReducerClass(job4.Job4Reducer.class);
        j4.setGroupingComparatorClass(job4.DecadeGroupingComparator.class);

        // secondary sort key
        j4.setMapOutputKeyClass(DoublePair.class);
        j4.setMapOutputValueClass(Text.class);

        // FINAL output should be readable text
        j4.setOutputKeyClass(Text.class);
        j4.setOutputValueClass(Text.class);
        j4.setOutputFormatClass(TextOutputFormat.class);

        j4.setNumReduceTasks(reducers);

        FileInputFormat.addInputPath(j4, out3);
        FileOutputFormat.setOutputPath(j4, out4);

        if (!j4.waitForCompletion(true)) {
            System.err.println("Job4 failed");
            System.exit(5);
        }

        System.out.println("DONE. Final output: " + out4);
        System.exit(0);
    }
}
