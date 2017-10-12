package com.refactorlabs.cs378.assign5;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;

/**
 * Map class for various WordCount examples that use the AVRO generated class WordCountData.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, AvroValue<WordCountData>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */
    private Text word = new Text();
    private String[] fields = {"user_id", "event_type", "event_timestamp", "city", "vin",
                               "vehicle_condition",	"year",	"make",	"model", "trim",
                               "body_style", "cab_style", "price", "mileage", "free_carfax_report", "features"};
    private HashSet<String> ignore = new HashSet<>();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] values = line.split("\t");
        if (ignore.isEmpty()) {
            ignore.add("event_timestamp");
            ignore.add("mileage");
            ignore.add("price");
            ignore.add("user_id");
            ignore.add("vin");
        }

        context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

        // For each word in the input line, emit a count of 1 for that word.
        for (int i=0; i < values.length; i++) {
            String field = fields[i];
            if (!ignore.contains(field)) {
                String val = values[i];
                if (field.equals("features")) {
                    String[] features = val.split(":");
                    for (String feature : features){
                        word.set(field + ":" + feature);
                        WordCountData.Builder builder = WordCountData.newBuilder();
                        builder.setCount(Utils.ONE);
                        context.write(word, new AvroValue<WordCountData>(builder.build()));
                    }
                }
                else {
                    word.set(field + ":" + val);
                    WordCountData.Builder builder = WordCountData.newBuilder();
                    builder.setCount(Utils.ONE);
                    context.write(word, new AvroValue<WordCountData>(builder.build()));
//                context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Words").increment(1L);
                }
            }
        }
    }
}
