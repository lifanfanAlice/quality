import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.logging.log4j.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriveMain implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(DriveMain.class);
    private Configuration conf;
    private PropertiesUtil propUtil;

    public DriveMain() {
        this.conf = new Configuration();
        this.propUtil = new PropertiesUtil("configure.properties");
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DriveMain(), args);
        System.exit(exitCode);
    }
    public int run(String[] args) throws Exception {

        logger.info("MapReduce Job Beginning.");
        String dbName = "ssb";
        String tableName = "customer";
        String sumField = "c_name";
        String outPath = "/Users/wangcheng/test_quality/1";
        logger.info("[Params] dbName:{}; tableName:{}, sumField:{}, outPath:{}", dbName, tableName, sumField, outPath);
        this.conf.set("sumField", sumField);
        /*String tmpJars = conf.get("tmpfiles");
        if (tmpJars == null) {
            tmpJars = "resources/";
        } else {
            tmpJars += "," + "resources/";
        }
        */
        Job job = this.setJobConfiguration(this.conf);
        HCatInputFormat.setInput(job,dbName,tableName);
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return (job.waitForCompletion(true) ? 0 : 1);


    }

    private Job setJobConfiguration(Configuration conf) throws Exception {
        try {
//            logger.info("enter setJobConfiguration");
            Job job = Job.getInstance(conf);
            job.setJarByClass(DriveMain.class);
            job.setInputFormatClass(HCatInputFormat.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(1);
            logger.info("setJobConfiguration successfully.");
            return job;
        } catch (Exception ex) {
            logger.error("setJobConfiguration: " + ex.getMessage());
            throw new Exception(ex);
        }
    }
    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}
