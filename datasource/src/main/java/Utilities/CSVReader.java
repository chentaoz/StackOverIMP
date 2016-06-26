package Utilities;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.csvreader.CsvReader;
public class CSVReader {
	public static void main(String[] args) {
		try {
			
			CsvReader products = new CsvReader("/Users/chentaoz/Documents/workspace/soimprove/src/test/resources/QueryResults.csv");
		
			products.readHeaders();

			while (products.readRecord())
			{
				String ID = products.get("Id");
				String typeId = products.get("PostTypeId");
				String created = products.get("CreationDate");
				String parent = products.get("ParentId");
			
				
				// perform program logic here
				System.out.println("Id" + ":" + ID);
				System.out.println("parent" + ":" + parent);
			
			}
	
			products.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	
}
