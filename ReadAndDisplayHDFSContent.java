import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public class ReadAndDisplayHDFSContent {
	public static void main(String args[]) throws MalformedURLException, IOException {

		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(configuration);
		Path filePath = new Path("hdfs://localhost:9000/user/acadgild/hadoop/word-count.txt");

		FSDataInputStream fsDataInputStream = fs.open(filePath);

		BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
	    String line = null;
	    while((line = br.readLine())!= null){
	        System.out.println(line);
	    }
	    fsDataInputStream.close();
	    br.close();
	    fs.close();
	    
	    }
}