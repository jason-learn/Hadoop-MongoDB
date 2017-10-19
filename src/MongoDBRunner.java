import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.Document;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;


/**
 * 操作mongodb的主类
 * @author hadoop
 *
 */
public class MongoDBRunner {
	/**
	 * mongodb转换到hadoop的一个bean对象
	 * @author hadoop
	 *
	 */
	 static class PersonMongoDBWritable implements MongoDBWritable{
		 private String name;
		 private Integer age;
		 private String sex = "";
		 private int count= 1;

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.name = in.readUTF();
			this.sex = in.readUTF();
			if(in.readBoolean()){
				this.age = in.readInt();
			}else{
				this.age = null;
			}
			this.count = in.readInt();
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(this.name);
			out.writeUTF(this.sex);
			if(this.age==null){
				out.writeBoolean(false);
			}
			else{
				out.writeInt(this.age);
			}
			out.writeInt(this.count);
		}
		
		/**
		 * 从mongodb中拿数据
		 */
		@Override
		public void readFields(DBObject dbObject) {
			 this.name = dbObject.get("name").toString();
			 //this.sex = dbObject.get("sex").toString();
			 if(dbObject.get("age")!=null){
				 this.age = Double.valueOf(dbObject.get("age").toString()).intValue();
					 
			 }else{
				 this.age = null;
			 }
			 
		}

		@Override
		public void write(MongoCollection dbCollection) {
			//创建器模式
			DBObject dbObject = BasicDBObjectBuilder.start().add("age",this.age).add("count", this.count).get();
			dbCollection.insertOne(Document.parse(dbObject.toString()));
		}
		 
	 }
	 
	 /**
	  * mapper
	  * @author hadoop
	  *
	  */
	 static class MongoDBMapper extends Mapper<LongWritable,PersonMongoDBWritable,IntWritable,PersonMongoDBWritable>{
		 @Override
		 protected void map(LongWritable key,PersonMongoDBWritable value,Mapper<LongWritable,PersonMongoDBWritable,IntWritable,PersonMongoDBWritable>.Context context) 
				 throws IOException, InterruptedException{		  
			 if(value.age==null){
				 System.out.println("guolv");
				 return;
			 }
			 
			 context.write(new IntWritable(value.age), value);
		 }
		 
	 }
	 /**
	  * reducer
	  * @author hadoop
	  *
	  */
	 static class MongoDBReducer extends Reducer<IntWritable,PersonMongoDBWritable,NullWritable,PersonMongoDBWritable>{
		 @Override
		 protected void reduce(IntWritable key, Iterable<PersonMongoDBWritable> values,Reducer<IntWritable,PersonMongoDBWritable,NullWritable,PersonMongoDBWritable>.Context context) throws IOException, InterruptedException{
			 int sum = 0;
			 for(PersonMongoDBWritable value:values){
				 sum+=value.count;
			 }
			 PersonMongoDBWritable personMongoDBWritable = new PersonMongoDBWritable();
			 personMongoDBWritable.age = key.get();
			 personMongoDBWritable.count = sum;
			 context.write(NullWritable.get(), personMongoDBWritable);
		 }
	 }
	 
	 public static void main(String[] args) throws Exception{
		 Configuration conf = new Configuration();
		 //设置inputformat的value类
		 conf.setClass("mapreduce.mongo.split.value.class", PersonMongoDBWritable.class, MongoDBWritable.class);
		 Job job = Job.getInstance(conf,"自定义input/outputFormat");
		 job.setJarByClass(MongoDBRunner.class);
		 job.setMapperClass(MongoDBMapper.class);
		 job.setReducerClass(MongoDBReducer.class);
		 job.setMapOutputKeyClass(IntWritable.class);            //mapper输出key
		 job.setMapOutputValueClass(PersonMongoDBWritable.class);//mapper输出value
		 job.setOutputKeyClass(NullWritable.class);              //reducer输出key
		 job.setOutputValueClass(PersonMongoDBWritable.class);   //reducer输出value
		 job.setInputFormatClass(mongodbInputFormat.class);      //设置inuputFormat
		 job.setOutputFormatClass(mongodbOutputFormat.class);    //设置outputFormat
		 
		 
		 job.waitForCompletion(true);
		 
	 }
}
