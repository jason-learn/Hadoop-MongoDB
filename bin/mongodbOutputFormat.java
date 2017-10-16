import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


/**
 * 自定义outputformat
 * @author jason
 *
 * @param <V>
 */
public class mongodbOutputFormat<V extends MongoDBWritable> extends OutputFormat<NullWritable,V> {
	
	/**
	 * 自定义mongodboutputformat
	 * @author hadoop
	 *
	 * @param <V>
	 */
	static class MongoDBRecordWriter<V extends MongoDBWritable> extends RecordWriter<NullWritable,V>{
		private MongoCollection dbCollection =null;
		public MongoDBRecordWriter(){
			super();
		}
		public MongoDBRecordWriter(TaskAttemptContext context) throws IOException{
			//获取mongo连接
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			//连接mongodb数据库
			MongoDatabase mongo = mongoClient.getDatabase("hadoop");		 
			//获取mongo集合
			dbCollection = mongo.getCollection("result");
			
		}
		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void write(NullWritable key, V value) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			value.write(this.dbCollection);
			
		}
		
	}
 

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(null,context);
	}
	
	
	@Override
	public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new MongoDBRecordWriter<>(context);
	}

	 

}
