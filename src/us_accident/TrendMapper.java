package us_accident;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrendMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final int DEFAULT_START_TIME_INDEX = 4;
    private static final int DEFAULT_SUNRISE_SUNSET_INDEX = 44;
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss", Locale.ROOT)
                    .withResolverStyle(ResolverStyle.STRICT);
    private static final LongWritable ONE = new LongWritable(1L);

    private final Text outputKey = new Text();
    private int startTimeIndex = DEFAULT_START_TIME_INDEX;
    private int sunriseSunsetIndex = DEFAULT_SUNRISE_SUNSET_INDEX;

    @Override
    protected void map(LongWritable offset, Text value, Context context)
            throws IOException, InterruptedException {
        List<String> columns = parseCsvLine(value.toString());
        if (columns == null) {
            context.getCounter("TrendMapper", "MALFORMED_CSV").increment(1);
            return;
        }

        if (readHeader(columns)) {
            return;
        }

        if (columns.size() <= Math.max(startTimeIndex, sunriseSunsetIndex)) {
            context.getCounter("TrendMapper", "MISSING_REQUIRED_COLUMNS").increment(1);
            return;
        }

        LocalDateTime timestamp;
        try {
            timestamp = LocalDateTime.parse(
                    columns.get(startTimeIndex).trim(), TIMESTAMP_FORMAT);
        } catch (DateTimeParseException exception) {
            context.getCounter("TrendMapper", "INVALID_TIMESTAMP").increment(1);
            return;
        }

        String period = columns.get(sunriseSunsetIndex).trim().toUpperCase(Locale.ROOT);
        if (!"DAY".equals(period) && !"NIGHT".equals(period)) {
            context.getCounter("TrendMapper", "INVALID_DAY_NIGHT_PERIOD").increment(1);
            return;
        }

        outputKey.set(String.format(Locale.ROOT, "MONTH:%02d", timestamp.getMonthValue()));
        context.write(outputKey, ONE);

        outputKey.set(String.format(Locale.ROOT, "HOUR:%02d", timestamp.getHour()));
        context.write(outputKey, ONE);

        outputKey.set("PERIOD:" + period);
        context.write(outputKey, ONE);
    }

    private boolean readHeader(List<String> columns) {
        int headerStartTimeIndex = -1;
        int headerSunriseSunsetIndex = -1;
        for (int index = 0; index < columns.size(); index++) {
            String column = columns.get(index).trim();
            if ("Start_Time".equals(column)) {
                headerStartTimeIndex = index;
            } else if ("Sunrise_Sunset".equals(column)) {
                headerSunriseSunsetIndex = index;
            }
        }

        if (headerStartTimeIndex < 0 || headerSunriseSunsetIndex < 0) {
            return false;
        }

        startTimeIndex = headerStartTimeIndex;
        sunriseSunsetIndex = headerSunriseSunsetIndex;
        return true;
    }

    private List<String> parseCsvLine(String line) {
        List<String> columns = new ArrayList<String>();
        StringBuilder field = new StringBuilder();
        boolean insideQuotes = false;
        boolean quoteClosed = false;

        for (int index = 0; index < line.length(); index++) {
            char character = line.charAt(index);
            if (insideQuotes) {
                if (character == '"') {
                    if (index + 1 < line.length() && line.charAt(index + 1) == '"') {
                        field.append('"');
                        index++;
                    } else {
                        insideQuotes = false;
                        quoteClosed = true;
                    }
                } else {
                    field.append(character);
                }
            } else if (character == ',') {
                columns.add(field.toString());
                field.setLength(0);
                quoteClosed = false;
            } else if (character == '"' && field.length() == 0 && !quoteClosed) {
                insideQuotes = true;
            } else if (quoteClosed) {
                return null;
            } else {
                field.append(character);
            }
        }

        if (insideQuotes) {
            return null;
        }

        columns.add(field.toString());
        return columns;
    }
}
