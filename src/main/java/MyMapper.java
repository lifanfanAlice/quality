import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;

public class MyMapper extends Mapper<WritableComparable, HCatRecord, Text, Text> {
}
