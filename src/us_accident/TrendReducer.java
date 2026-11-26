package us_accident;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrendReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final LongWritable total = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0L;
        for (LongWritable value : values) {
            sum += value.get();
        }

        total.set(sum);
        context.write(key, total);
    }
}
