package org.warcbase.cwi.bulkloading;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.warcbase.cwi.arcUtils.ArcInputFormat;
import org.warcbase.cwi.arcUtils.ArcRecord;
import org.warcbase.data.Util;


public class EstimaeTableSplits extends Configured implements Tool {

	private static final Logger LOG = Logger
			.getLogger(EstimaeTableSplits.class);
	private static final String ARGNAME_INPATH = "-in";
	private static final String ARGNAME_OUTPATH = "-out";
	private static final String ARGNAME_REDUCERS = "-reducers";
	

	public static class myMapper extends
			Mapper<Text, ArcRecord, Text, IntWritable> {
		Log log = LogFactory.getLog(myMapper.class);

		Text outKey = new Text();
		IntWritable one = new IntWritable(1);

		public void map(Text key, ArcRecord value, Context context)
				throws IOException {

			try {

				// get the url from the map (value:ARC record)

				String url = value.getURL();
				String host =Util.reverseHostname(url);
				String firstChar = "";
				if (host.length() > 1){
					firstChar = host.substring(0, 1);
					
				}

				outKey.set(firstChar);

				context.write(outKey, one);

			} catch (Throwable e) {

				LOG.error("Caught Exception", e);

			}
		}
	}
	
	public static class myReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		   
		       public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		         throws IOException, InterruptedException {
		           int sum = 0;
		           for (IntWritable val : values) {
		               sum += val.get();
		           }
		           context.write(key, new IntWritable(sum));
		       }
		    }

	public void usage() {
		System.out.println("\n  org.commoncrawl.examples.NuArcsRecords \n"
				+ ARGNAME_INPATH + "\t" + " <inputpath>\n"

				+ ARGNAME_OUTPATH + "\t" + " <outputpath>\n" + ARGNAME_REDUCERS
				+ "\t" + " <numreducers>\n");
		System.out.println("");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	/**
	 * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
	 * 
	 * @param args
	 *            command line parameters, less common Hadoop job parameters
	 *            stripped out and interpreted by the Tool class.
	 * @return 0 if the Hadoop job completes successfully, 1 if not.
	 */
	@Override
	public int run(String[] args) throws Exception {

		String inputPath = null;
		String outputPath = null;
		int reducers = 1;

		// Read the command line arguments. We're not using GenericOptionsParser
		// to prevent having to include commons.cli as a dependency.
		for (int i = 0; i < args.length; i++) {
			try {
				if (args[i].equals(ARGNAME_INPATH)) {
					inputPath = args[++i];
				} else if (args[i].equals(ARGNAME_OUTPATH)) {
					outputPath = args[++i];
				} else if (args[i].equals(ARGNAME_REDUCERS)) {
					reducers = Integer.parseInt(args[++i]);
				} else {
					LOG.warn("Unsupported argument: " + args[i]);
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				usage();
				throw new IllegalArgumentException();
			}
		}

		if (inputPath == null || outputPath == null) {
			usage();
			throw new IllegalArgumentException();
		}

		// Create the Hadoop job.
		Configuration conf = getConf();
		// conf.set("mapred.map.child.java.opts",
		// "-Xmx2048m -XX:-UseGCOverheadLimit");
		conf.set("mapred.map.child.java.opts",
				"-Xmx4096m -XX:-UseGCOverheadLimit");
		Job job = new Job(conf);

		job.setJarByClass(EstimaeTableSplits.class);
		job.setNumReduceTasks(reducers);

		// Scan the provided input path for ARC files.
		LOG.info("setting input path to '" + inputPath + "'");
		

		FileInputFormat.addInputPath(job, new Path(inputPath));
		

		// Set the path where final output 'part' files will be saved.
		LOG.info("setting output path to '" + outputPath + "'");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);

		// Set which InputFormat class to use.
		job.setInputFormatClass(ArcInputFormat.class);

		// Set which OutputFormat class to use.
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the output data types.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Set which Mapper and Reducer classes to use.
		job.setMapperClass(EstimaeTableSplits.myMapper.class);
		job.setReducerClass(EstimaeTableSplits.myReducer.class);

		if (job.waitForCompletion(true)) {
			return 0;
		} else {
			return 1;
		}
	}

	/**
	 * Main entry point that uses the {@link ToolRunner} class to run the
	 * example Hadoop job.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new EstimaeTableSplits(),
				args);
		System.exit(res);
	}
}
