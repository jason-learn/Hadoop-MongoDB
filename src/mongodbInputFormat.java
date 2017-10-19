import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import com.mongodb.util.JSON;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import org.bson.Document;

 
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

/**
 * 自定义mongodb inputformat类
 * @author jason
 *
 * @param <K>
 * @param <V>
 */

public class mongodbInputFormat<V extends MongoDBWritable> extends InputFormat<LongWritable, V> {
	
	
 
	
	/**
	 * 获取具体的reader类
	 */
	@Override
	public RecordReader<LongWritable, V> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
	 
		return new MongoDBRecordReader(split,context);
	}
	
	/**
	 * 一个空的mongodb自定义数据类型
	 * @author jason
	 *
	 */
	static class NullMongoDBWritable implements MongoDBWritable{

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			
		}

	 

		@Override
		public void write(MongoCollection dbCollection) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void readFields(DBObject dbObject) {
			// TODO Auto-generated method stub
			
		}
		
	}
	/**
	 * 自定义mongodbreader类（内部类）
	 * @author jason
	 *
	 * @param <V>
	 */
	static class MongoDBRecordReader<V extends MongoDBWritable> extends RecordReader<LongWritable,V>{
		private MongoDBInputSplit split;
		//private Configuration conf;
		private MongoCursor<Document> cursor;
		private int index;
		private LongWritable key;
		private V value;
		
		public MongoDBRecordReader(){	
			super();
		 
		}
		public MongoDBRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			super();
			this.initialize(split, context);
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			//this.dbCursor.getCollection().getDB();
			this.cursor.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return this.key;
		}

		@Override
		public V getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return this.value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			this.split = (MongoDBInputSplit) split;
			Configuration conf = context.getConfiguration();
			key = new LongWritable();
			Class clz = conf.getClass("mapreduce.mongo.split.value.class",NullMongoDBWritable.class);
			value = (V)ReflectionUtils.newInstance(clz, conf);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(this.cursor == null){
				//获取mongo连接
				MongoClient mongoClient = new MongoClient("localhost", 27017);
				//连接mongodb数据库
				MongoDatabase mongo = mongoClient.getDatabase("hadoop");		 
				//获取mongo集合
				MongoCollection<Document> dbcollection = mongo.getCollection("person");
				//获取dbcursor对象
				cursor =  dbcollection.find().skip((int)this.split.start).limit((int)this.split.getLength()).iterator();
			}
			boolean hasNext = this.cursor.hasNext();
			if(hasNext){
				DBObject dbObject= (DBObject)JSON.parse(this.cursor.next().toJson());
				//DBObject obj = (DBObject)JSON.parse(this.cursor.next().toJson());
				this.key.set(this.split.start + index);
				this.index++;
				this.value.readFields(dbObject);
			}
 
			return hasNext;
		}
		
	}
	
	/**
	 * mongodb自定义inputsplit
	 * @author jason
	 *
	 */
	static class MongoDBInputSplit extends InputSplit implements Writable{
		private long start; //起始位置，包含
		private long end;   //终止位置，不包含
		
		public MongoDBInputSplit(){
			super();
		}
		
		public MongoDBInputSplit(long start,long end){
			super();
			this.start = start;
			this.end = end;
		}
		
		@Override
		public long getLength() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return end-start;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return new String[0];
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.start = in.readLong();
			this.end = in.readLong();
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(this.start);
			out.writeLong(this.end);
		}
		
	}
	/**
	 * 获取分片信息
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		//获取mongo连接
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		//连接mongodb数据库
		MongoDatabase mongo = mongoClient.getDatabase("hadoop");		 
		//获取mongo集合
		MongoCollection dbcollection = mongo.getCollection("person");
		//每两条数据一个mapper
		int chunkSize = 2;
		long size = dbcollection.count();  //获取mongodb对于collection的数据条数
		long chunk = size / chunkSize;    //计算mapper个数
		List<InputSplit> list = new ArrayList<InputSplit>();
		for(int i =0;i< chunk;i++){
			if(i+1 == chunk){
				list.add(new MongoDBInputSplit(i*chunkSize,size+1));
			}else{
				list.add(new MongoDBInputSplit(i*chunkSize,i*chunkSize+chunkSize));
			}
			
		}
		
		return list;
	}

}
