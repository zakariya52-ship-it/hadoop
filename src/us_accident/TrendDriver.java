package us_accident;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrendDriver {
    private static final String DEFAULT_INPUT = "data/input/us_accident_cleaned.csv";
    private static final String DEFAULT_OUTPUT = "data/output/us_accident_trends";

    public static void main(String[] args) throws Exception {
        if (args.length > 2) {
            System.err.println("Usage: TrendDriver [input path] [output path]");
            System.exit(2);
        }

        String input = args.length > 0 ? args[0] : DEFAULT_INPUT;
        String output = args.length > 1 ? args[1] : DEFAULT_OUTPUT;

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "US Accidents Monthly, Hourly, and Period Trends");

        job.setJarByClass(TrendDriver.class);
        job.setMapperClass(TrendMapper.class);
        job.setCombinerClass(TrendReducer.class);
        job.setReducerClass(TrendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
