package org.warcbase.cwi.bulkloading;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.warcbase.cwi.arcUtils.ArcInputFormat;
import org.warcbase.cwi.arcUtils.ArcRecord;
import org.warcbase.data.Util;

public class HFilesOfArcFiles extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(HFilesOfArcFiles.class);

  final static SimpleDateFormat dateWARC = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");

  public static class MyMapper extends Mapper<Text, ArcRecord, ImmutableBytesWritable, Put> {

    ImmutableBytesWritable outKey = new ImmutableBytesWritable();

    Put outValue;
    String tableName = "";
    final static byte[] COL_FAM = "outlinks".getBytes();
    private static final String[] FAMILIES = { "content", "type" };
    public static final int MAX_CONTENT_SIZE = 1024 * 1024;

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      String tableName = conf.get("hbase.table.name");

    }

    public void map(Text key, ArcRecord record, Context context) throws IOException,
        InterruptedException {

      try {

        // get the url from the map (value:ARC record)
        String url = record.getURL();
        String contetType = record.getContentType();
        String archivedDate = record.getArchivedDateStr();
        // .substring(0, 8);

        // extract HTML
        // String content = value.getParsedHTML().toString();
        byte[] content = record.getPayload();

        if (url != null && contetType == null) {
          contetType = "text/plain";
        }

        if (contetType != null && url != null && content.length <= MAX_CONTENT_SIZE) {

          // key: reverse of the url
          outKey.set(Util.reverseHostname(url).getBytes());
          outValue = new Put(Util.reverseHostname(url).getBytes());

          outValue.add(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes(archivedDate), content);
          outValue.add(Bytes.toBytes(FAMILIES[1]), Bytes.toBytes(archivedDate),
              Bytes.toBytes(contetType));

          context.write(outKey, outValue);

        }

      } catch (Throwable e) {

        if (e.getClass().equals(OutOfMemoryError.class)) {

          System.gc();
        }

        LOG.error("Caught Exception", e);

      }

    }

  }

  private static final String ARGNAME_INPATH = "-in";
  private static final String ARGNAME_OUTPATH = "-out";
  private static final String ARGNAME_TABLE = "-table";

  public void usage() {
    System.out.println("\n  FindContext \n"

    + ARGNAME_INPATH + " <inputpath>\n" + ARGNAME_OUTPATH + " <out>\n" + ARGNAME_TABLE
        + " <table>\n");
    System.out.println("");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  @SuppressWarnings("deprecation")
  @Override
  public int run(String[] args) throws Exception {
    String inPath = null;
    String outPath = null;
    String hbaseTable = null;
    String metaFile = "";

    // Read the command line arguments.

    for (int i = 0; i < args.length; i++) {
      try {
        if (args[i].equals(ARGNAME_INPATH)) {
          inPath = args[++i];
        } else if (args[i].equals(ARGNAME_OUTPATH)) {
          outPath = args[++i];
        } else if (args[i].equals(ARGNAME_TABLE)) {
          hbaseTable = args[++i];
        } else {
          LOG.warn("Unsupported argument: " + args[i]);
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        usage();
        throw new IllegalArgumentException();
      }
    }

    if (inPath == null || outPath == null || hbaseTable == null) {
      usage();
      throw new IllegalArgumentException();
    }

    Configuration conf = new Configuration();
    // HBaseConfiguration.create();
    DistributedCache.addCacheFile(new URI(metaFile), conf);

    conf.set("mapreduce.map.java.opts", "-Xmx8g");

    conf.set("mapreduce.reduce.java.opts", "-Xmx8g");
    conf.set("mapred.child.java.opts", "-Xmx10g -XX:-UseGCOverheadLimit");
    conf.setLong("mapred.task.timeout", 4 * 3600 * 000);
    // -Xmx1024m -XX:-UseGCOverheadLimit

    conf.set("hbase.table.name", hbaseTable);

    Job job = new Job(conf, "hbase bulk loading");
    job.setJarByClass(HFilesOfArcFiles.class);

    HBaseConfiguration.addHbaseResources(conf);

    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);

    job.setInputFormatClass(ArcInputFormat.class);
    HTable hTable = new HTable(conf, hbaseTable);

    // Auto configure partitioner and reducer
    HFileOutputFormat.configureIncrementalLoad(job, hTable);

    FileInputFormat.addInputPath(job, new Path(inPath));
    FileOutputFormat.setOutputPath(job, new Path(outPath));

    TableMapReduceUtil.addDependencyJars(job);

    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    LOG.info("Running " + HFilesOfArcFiles.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new HFilesOfArcFiles(), args);

  }

}
