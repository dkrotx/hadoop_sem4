import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class HBaseBulkDemo extends Configured implements Tool {
    private static Logger LOG = LoggerFactory.getLogger(HBaseBulkDemo.class);

    @Override
    public int run(String[] args) throws Exception {
        String input_path = args[0];
        TableName output_table = TableName.valueOf(args[1]);
        Path bulks_dir = new Path(args[2]);

        Job job = GetJobConf(input_path, output_table, bulks_dir);
        if (!job.waitForCompletion(true))
            return 1;

        LOG.info("loading hfiles");
        Connection connection = ConnectionFactory.createConnection(getConf());
        Table table = connection.getTable(output_table);

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
        loader.doBulkLoad(
                bulks_dir,
                connection.getAdmin(),
                table,
                connection.getRegionLocator(output_table)
        );

        return 0;
    }

    Job GetJobConf(String input_path, TableName output_table, Path bulks_dir) throws IOException {
        Job job = Job.getInstance(getConf(), "HBaseBulkDemo");
        job.setJarByClass(HBaseBulkDemo.class);
        FileInputFormat.addInputPath(job, new Path(input_path));

        job.setMapperClass(TextInputMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        Connection connection = ConnectionFactory.createConnection(getConf());
        HTableDescriptor tbl = new HTableDescriptor(output_table);
        HFileOutputFormat2.configureIncrementalLoad(job, tbl, connection.getRegionLocator(output_table));
        HFileOutputFormat2.setOutputPath(job, bulks_dir);

        return job;
    }

    static public class TextInputMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\t");
            if (items.length != 2) {
                context.getCounter("COMMON", "bad_input").increment(1);
                return;
            }

            String url = items[0];
            String doc = new String(Base64.decodeBase64(items[1]), "UTF8");

            /* write your code here */
        }
    }


    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(HBaseConfiguration.create(), new HBaseBulkDemo(), args);
        System.exit(rc);
    }
}
