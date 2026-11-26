import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SeasonMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text seasonKey = new Text();
    
    // Pattern to extract YYYY-MM-DD timestamps
    private static final Pattern DATE_PATTERN = Pattern.compile("\\b(\\d{4})-(\\d{2})-\\d{2}\\b");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip CSV Header row
        if (key.get() == 0 && line.contains("Start_Time")) {
            return;
        }

        Matcher matcher = DATE_PATTERN.matcher(line);
        if (matcher.find()) {
            String month = matcher.group(2); // Extracts month digits (MM)
            String season = getSeason(month);
            if (season != null) {
                seasonKey.set(season);
                context.write(seasonKey, one);
            }
        }
    }

    private String getSeason(String month) {
        switch (month) {
            case "12": case "01": case "02": return "1. Winter";
            case "03": case "04": case "05": return "2. Spring";
            case "06": case "07": case "08": return "3. Summer";
            case "09": case "10": case "11": return "4. Fall";
            default: return null;
        }
    }
}