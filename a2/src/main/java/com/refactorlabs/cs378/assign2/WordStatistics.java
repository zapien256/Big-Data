package com.refactorlabs.cs378.assign2;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

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
			Map<String, Long> wordMap = new TreeMap<String, Long>();

			// Go through each word(token) and map it
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				token = token.toLowerCase().replaceAll("^\\p{Punct}+|\\p{Punct}+$","");

				// Split tokens that are words paired with footnotes (e.g. "parry[270]")
				if (!token.startsWith("[") && token.endsWith("]")) {
					int index = token.indexOf('[');
					String token2 = token.substring(index);
					token = token.substring(0,index);
					mapAdd(wordMap, token2);
				}

				mapAdd(wordMap, token);
			}

			for (String k : wordMap.keySet()) {
				word.set(k);
				long val = wordMap.get(key);

				// Save Values from wordCount via a WordStatisticsWritable object
				WordStatisticsWritable out = new WordStatisticsWritable();
				out.setLongs(val, 1L);

				// Write to context
				context.write(word, out);
			}
		}

		private void mapAdd(Map<String, Long> wordMap, String token) {
			if (!wordMap.containsKey(token))
				wordMap.put(token, 1L);
			else
				wordMap.put(token, wordMap.get(token) + 1);
		}
	}

	/**
	 * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for word statistics.
	 */
	public static class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

		@Override
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
				throws IOException, InterruptedException {

			long count = 0;
			long countSquared = 0;
			long docCount = 0;

			for (WordStatisticsWritable ws: values) {
				count += ws.getCount();
				countSquared += ws.getCountSquared();
				docCount += ws.getDocCount();
			}

			WordStatisticsWritable out = new WordStatisticsWritable();
			out.setLongs(count, docCount);

			double mean = getMean(count, docCount);
			double variance = getVariance(countSquared, docCount, mean);

			out.setMean(mean);
			out.setVariance(variance);

			// Emit the document count, mean, variance for the word.
			context.write(key, out);
		}

		private double getMean(double count, double docCount){
			double result = count/docCount;
			return result;
		}

		private double getVariance(double countSquared, double docCount, double mean) {
			double result = (countSquared / docCount) - (mean * mean);
			return result;
		}
	}

	/**
	 * The WordStatisticsWritable class for word statistics.
	 */
	public static class WordStatisticsWritable implements Writable {

		/*Instance Variables*/
		private long docCount;
		private long count;
		private long countSquared;
		private double mean;
		private double variance;

		public WordStatisticsWritable() {
			this.mean = 0;
			this.variance = 0;
		}

		public void setLongs(long count, long docCount) {
			this.count = count;
			this.docCount = docCount;
			this.countSquared = countSquared;
		}

		public void setDoubles(double mean, double variance) {
			this.mean = mean;
			this.variance = variance;
		}

		public void write(DataOutput out) throws IOException {
			// Write long Values
			out.writeLong(docCount);
			out.writeLong(count);
			out.writeLong(countSquared);

			// Write double Values
			out.writeDouble(mean);
			out.writeDouble(variance);
		}

		public void readFields(DataInput in) throws IOException {

			// Process long Values
			long valLong = in.readLong();
			this.docCount = valLong;
			valLong = in.readLong();
			this.count = valLong;
			valLong = in.readLong();
			this.countSquared = valLong;

			// Process double values
			double valDouble = in.readDouble();
			this.mean = valDouble;
			this.variance = in.readDouble();
		}

		public void setMean(double mean) {
			this.mean = mean;
		}

		public void setVariance(double variance) {
			this.variance = variance;
		}

		public double getMean() { return mean; }

		public double getVariance() { return variance; }

		public long getCount() { return count; }

		public long getDocCount() { return docCount; }

		public long getCountSquared() { return countSquared; }

		@Override
		public String toString() {
			return docCount + ", " + mean + ", " + variance;
		}

		public boolean equals(WordStatisticsWritable other) {
			//If Equal
			if (this.docCount == other.docCount
					&& this.count == other.count
					&& this.mean == other.mean
					&& this.variance == other.variance)
				return true;

			//If Not Equal
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
