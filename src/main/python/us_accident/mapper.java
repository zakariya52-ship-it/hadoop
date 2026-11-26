package us_accident;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text state = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // 1. التجاوز عن الـ Header
        if (key.get() == 0 && line.startsWith("ID")) {
            return;
        }

        // 2. تقسيم السطر بـ CSV Regex يتعامل مع الكومات داخل النماذج المزدوجة
        String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        // 3. استخراج رمز الولاية (Index 14) والتأكد من أنه يتكون من حرفين (مثل CA, NY, TX)
        if (columns.length > 14 && !columns[14].trim().isEmpty()) {
            String stateCode = columns[14].trim().replaceAll("\"", "");
            if (stateCode.length() == 2) {
                state.set(stateCode);
                context.write(state, one);
            }
        }
    }
}