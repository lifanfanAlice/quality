import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;

public class MyMapper extends Mapper<WritableComparable, HCatRecord, Text, Text> {
    HCatSchema schema;
    Text guid;
    Text one;

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
        guid = new Text();
        one = new Text();
        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
    }

    @Override
    protected void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {
        guid.set(value.getString("guid", schema));
        context.write(guid, one);
    }
}
