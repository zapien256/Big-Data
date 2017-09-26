// package com.refactorlabs.cs378.assign1;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * MapReduce program that performs word statistics.
 *
 * @author Jorge Zapien-Diaz (jaz747@cs.utexas.edu)
 */
public class WordStatistics extends Configured implements Tool {

	/**
	 * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for word statistics
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
            
			// Keep a count for each word
			Map<String, Long> wordCount = new TreeMap<String, Long>(); 

			// Go through each word(token) and map it
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if (!wordMap.containsKey(token))
					wordMap.put(token, 1L);
				else
					wordMap.put(token, wordMap.get(token) + 1);
			}

			for (String key : wordCount.keySet()) {
				word.set(key);
				long value = wordCount.get(key);

				// Save Values from wordCount via a WordStatisticsWritable object
				WordStatisticsWritable out = new WordStatisticsWritable(value, 1L);

				// Write to context
				context.write(word, out);
			}
		}
	}

	/**
	 * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for word statistics.
	 */
	public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0L;

			context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			// Sum up the counts for the current word, specified in object "key".
			for (LongWritable value : values) {
				sum += value.get();
			}
			// Emit the total count for the word.
			context.write(key, new LongWritable(sum));
		}
	}

	/**
	 * The WordStatisticsWritable class for word statistics.
	 */
	public static class WordStatisticsWritable implements Writable<Text, LongWritable, Text, LongWritable> {
		
		/*Instance Variables*/
		private long docCount;
		private long count;
		private double mean;
		private double variance;
		
		public WordStatisticsWritable(long count, long docCount) {
			this.docCount = docCount;
			this.count = count;
			this.mean = 0;
			this.variance = 0;
		}

		public void write(DataOutput out) throws IOException {
        	out.writeLong(count);
        	out.writeDouble(mean);
			out.writeDouble(variance);
      	}
       
		public void readFields(DataInput in) throws IOException {
			counter = in.readInt();
			timestamp = in.readLong();
		}

		public String toString() {
		    return count + ", " + mean + ", " + variance;
		}
		
        public boolean equals(WordStatisticsWritable other) {
            return false;
        }
	}
	

	/**
	 * The run method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "WordCount");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistics.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticsWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
        job.setCombinerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		int res = ToolRunner.run(new WordStatistics(), args);
		System.exit(res);
	}

}
