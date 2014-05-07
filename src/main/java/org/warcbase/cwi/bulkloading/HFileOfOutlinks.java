package org.warcbase.cwi.bulkloading;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.warcbase.cwi.arcUtils.ArcInputFormat;
import org.warcbase.cwi.arcUtils.ArcRecord;
import org.warcbase.cwi.pig.piggybank.GetHostName;
import org.warcbase.data.Util;

public class HFileOfOutlinks extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(HFileOfOutlinks.class);

  final static SimpleDateFormat dateWARC = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");

  public static class MyMapper extends Mapper<Text, ArcRecord, ImmutableBytesWritable, Put> {

    ImmutableBytesWritable outKey = new ImmutableBytesWritable();

    Put put;
    String tableName = "";
    final static byte[] COL_FAM = "outlinks".getBytes();
    private static final String[] FAMILIES = { "links", "meta" };
    Set<String> seedList = new HashSet<String>();
    Map<String, String> UnescoCodeMapping = new HashMap<String, String>(30);
    Map<String, String> ApprovedMapping = new HashMap<String, String>(30);

    @SuppressWarnings("deprecation")
    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();

      try {
        BufferedReader br = null;
        // read seeds file
        Path seedFile = DistributedCache.getLocalCacheFiles(conf)[0];
        br = new BufferedReader(new FileReader(seedFile.toString()));
        String line = null;

        while ((line = br.readLine()) != null) {

          seedList.add(line);

        }
        br.close();

        // read seeds meta
        Path metaFile = DistributedCache.getLocalCacheFiles(conf)[1];
        br = new BufferedReader(new FileReader((metaFile.toString())));

        br.readLine();

        while ((line = br.readLine()) != null) {

          // logger.info(line );
          String[] lineComp = line.trim().split("\t");
          if (lineComp.length > 1) {
            // split and save entry

            String approvedDate = lineComp[4].trim().replaceAll("-", "").substring(0, 8);

            GetHostName ghn = new GetHostName();
            Tuple urlT = TupleFactory.getInstance().newTuple(1);

            urlT.set(0, lineComp[3].trim());

            UnescoCodeMapping.put(ghn.exec(urlT), lineComp[0].trim());
            ApprovedMapping.put(ghn.exec(urlT), approvedDate);

          }

        }
        br.close();

      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }

    private Boolean checkSeedList(String host, Set<String> hashT)
        throws java.net.MalformedURLException {
      Iterator<String> iterator = hashT.iterator();
      Boolean value = false;
      while (iterator.hasNext()) {
        String key = iterator.next();
        // System.out.println(key);
        // if(url.contains(key)) {
        if (host.equals(key) || host.contains("." + key)) {
          // logger.warn(url+" equals "+key);
          value = true;
          return value;
        }
      }
      return value;
    }

    private String getHostMeta(String host, Map<String, String> hashT)
        throws java.net.MalformedURLException {
      Iterator<String> iterator = hashT.keySet().iterator();
      String value = null;
      while (iterator.hasNext()) {
        String key = iterator.next().toString();
        // if(url.contains(key)) {
        if (host.equals(key) || host.contains("." + key)) {
          // logger.warn(url+" equals "+key);
          value = hashT.get(key).toString();
          return value;
        }
      }
      return null;
    }

    public void map(Text key, ArcRecord value, Context context) throws IOException,
        InterruptedException {

      String srcUnesco = null;
      String contentHash = null;
      String archivedDate = null;
      String srcHost = null;
      String targetHost = null;
      String targetUnesco = null;
      Boolean srcInSeed = false;
      Boolean targetInSeed = false;
      try {

        if ((value.getContentType().contains("html"))) {
          // get the url from the map (value:ARC record)
          String src = value.getURL();
          archivedDate = value.getArchivedDateStr().substring(0, 8);

          // get src host name

          GetHostName ghn = new GetHostName();
          Tuple urlT = TupleFactory.getInstance().newTuple(1);

          urlT.set(0, src);
          srcHost = ghn.exec(urlT);
          if (srcHost != null) {
            srcInSeed = checkSeedList(srcHost, seedList);
            if (srcInSeed) {
              srcUnesco = getHostMeta(srcHost, UnescoCodeMapping);

            }
          }

          // extract HTML
          String html = value.getParsedHTML().toString();
          MessageDigest md = MessageDigest.getInstance("MD5");
          contentHash = md.digest(html.getBytes("UTF-8")).toString();

          if (html != null && src != null) {

            outKey.set(Bytes.toBytes(Util.reverseHostname(src) + "|" + archivedDate));
            put = new Put(Bytes.toBytes(Util.reverseHostname(src) + "|" + archivedDate));
            Document doc = Jsoup.parse(html, "UTF-8");
            Elements outLinks = doc.select("a[href]");
            int count = 0;
            for (Element link : outLinks) {
              String linkHref = link.attr("abs:href");
              String linkText = link.text();
              if (linkHref.length() > 0 && linkText.length() > 0 && !linkHref.startsWith("mailto")) {
                Tuple targetT = TupleFactory.getInstance().newTuple(1);

                targetT.set(0, linkHref);
                targetHost = ghn.exec(targetT);
                if (targetHost != null) {
                  targetInSeed = checkSeedList(targetHost, seedList);

                  if (targetInSeed) {

                    targetUnesco = getHostMeta(targetHost, UnescoCodeMapping);
                  }
                }

                count++;
                put.add(
                    Bytes.toBytes(FAMILIES[0]),
                    Bytes.toBytes(linkHref),
                    Long.parseLong(archivedDate),
                    Bytes.toBytes(linkText.trim().replaceAll("\\s+", " ").replaceAll("\n", "")
                        .replaceAll("[^ A-Za-z\\,\\']+", "")
                        + "|" + targetUnesco + "|" + targetInSeed));

              }

            }
            if (srcUnesco != null) {
              put.add(Bytes.toBytes(FAMILIES[1]), Bytes.toBytes("unesco"),
                  Long.parseLong(archivedDate), Bytes.toBytes(srcUnesco));
            } else {
              srcUnesco = "null";
              put.add(Bytes.toBytes(FAMILIES[1]), Bytes.toBytes("unesco"),
                  Long.parseLong(archivedDate), Bytes.toBytes(srcUnesco));
            }

            put.add(Bytes.toBytes(FAMILIES[1]), Bytes.toBytes("inSeeds"),
                Long.parseLong(archivedDate), Bytes.toBytes(srcInSeed));
            put.add(Bytes.toBytes(FAMILIES[1]), Bytes.toBytes("archiveDateStr"),
                Long.parseLong(archivedDate), Bytes.toBytes(archivedDate));
            put.add(Bytes.toBytes(FAMILIES[1]), Bytes.toBytes("hash"),
                Long.parseLong(archivedDate), Bytes.toBytes(contentHash));
            LOG.info("fount outlinks in :" + Util.reverseHostname(src) + "\t" + count);
            context.write(outKey, put);

          }
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

  private static final String ARGNAME_META = "-meta";
  private static final String ARGNAME_SEED = "-seeds";
  

  public void usage() {
    System.out.println("\n  FindContext \n"

    + ARGNAME_INPATH + " <inputpath>\n" + ARGNAME_OUTPATH + " <out>\n" + ARGNAME_TABLE
        + " <table>\n" + ARGNAME_SEED + "\t" + " <seedfile>\n" + ARGNAME_META + " <arcs-meta>\n");
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
    String seedFile = "";

    // Read the command line arguments.

    for (int i = 0; i < args.length; i++) {
      try {
        if (args[i].equals(ARGNAME_INPATH)) {
          inPath = args[++i];
        } else if (args[i].equals(ARGNAME_OUTPATH)) {
          outPath = args[++i];
        } else if (args[i].equals(ARGNAME_TABLE)) {
          hbaseTable = args[++i];
        } else if (args[i].equals(ARGNAME_SEED)) {
          seedFile = args[++i];
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
    DistributedCache.addCacheFile(new URI(seedFile), conf);
    DistributedCache.addCacheFile(new URI(metaFile), conf);

    conf.set("mapreduce.map.java.opts", "-Xmx8g");

    conf.set("mapreduce.reduce.java.opts", "-Xmx8g");
    conf.set("mapred.child.java.opts", "-Xmx10g -XX:-UseGCOverheadLimit");
    conf.setLong("mapred.task.timeout", 4 * 3600 * 000);
    // -Xmx1024m -XX:-UseGCOverheadLimit

    conf.set("hbase.table.name", hbaseTable);

    Job job = new Job(conf, "hbase bulk loading");
    job.setJarByClass(HFileOfOutlinks.class);

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
    LOG.info("Running " + HFileOfOutlinks.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new HFileOfOutlinks(), args);

  }

}
