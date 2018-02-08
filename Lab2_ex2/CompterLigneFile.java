package cs.bigdata.Tutorial2;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class CompterLigneFile {

	public static void main(String[] args) throws IOException {
		
		String localSrc = "/home/cloudera/Lab2/arbres.csv";
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			int count_line = 0;
			
			while (line !=null){
				// Process of the current line
				count_line +=1;
				// go to the next line
				line = br.readLine();
			}
			System.out.println("The number of lines:");
			System.out.println( count_line);
		}
		finally{
			//close the file
			
			in.close();
			fs.close();
		}
 
		
		
	}

}
