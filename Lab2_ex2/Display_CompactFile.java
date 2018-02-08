package cs.bigdata.Tutorial2;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


public class Display_CompactFile {

	public static void main(String[] args) throws IOException {
		
		String localSrc = "/home/cloudera/Lab2/USAF_src.txt";
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			int count_line =0;
			String line = br.readLine();
			count_line += 1;
			
			while (line !=null ){
				// Process of the current line
				
				if (count_line > 22){
					String USAF_code = line.substring(0, 6);
					String name = line.substring(13, 42);
					String FIPS = line.substring(43, 45);
					String altitude = line.substring(74, 81);
					System.out.println("USAF Code: "+USAF_code+", Station name: "+name+", Country Num: "+FIPS+", Altitude (in meters): "+altitude);
				}
				// go to the next line
				line = br.readLine();
				count_line += 1;
			}
		}

		finally{
			//close the file
			in.close();
			fs.close();
		}
		
	}

}
