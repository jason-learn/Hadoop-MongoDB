import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;

/**
 * mongodb自定义数据类型
 * @author jason
 *
 */

public interface MongoDBWritable extends Writable{
	/**
	 * 从mongodb中读取数据,用于从mongodb中读取的dbobject转化为mongodbinput的输入value
	 * @param dbObject
	 */
	public void readFields(DBObject dbObject);
	/**
	 * 向mongodb中写入数据
	 * @param dbCollection
	 */
	public void write(MongoCollection dbCollection);
	 

}
