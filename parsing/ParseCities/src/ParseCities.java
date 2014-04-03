import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class ParseCities {
	
	private ArrayList<String> cities;
	
	public ParseCities() {
		cities = new ArrayList<String>();
	}
	
	private void toOneFile(String input, String output) throws IOException{
		String line = "";
		BufferedReader br = new BufferedReader(new FileReader(input));
		
		while ((line = br.readLine()) != null) {
			String[] terms = line.split("\\s+");
			if(terms.length > 4){
				StringBuilder b = new StringBuilder();
				b.append(terms[0]);
				b.append(" ");
				b.append(terms[1]);
				b.append(" ");
				for(int i = 2; i < terms.length - 2; i++){
					b.append(terms[i]);
					b.append("_");
				}
				b.append(terms[terms.length - 2]);
				b.append(" ");
				b.append(terms[terms.length - 1]);
				
				cities.add(b.toString());
			} else {
				cities.add(line);
			}
		}
		br.close();
		
		System.out.printf("\nGoing to write the changed input set to a new file called %s\n", String.format("%s", output));
		
		//Writing to file
		File file = new File(output);
		BufferedWriter bw = new BufferedWriter(new FileWriter(file));
		Iterator<String> cityIterator = cities.iterator();
		while (cityIterator.hasNext()) {
			bw.write(String.format("%s\n", cityIterator.next()));
		}
		bw.close();
		cities.clear();
	}
	
	private void toMultipleFiles(String input, String output) throws IOException{
		int countries = 0;
		String line = "", previousCountry = "";
		BufferedReader br = new BufferedReader(new FileReader(input));
		while ((line = br.readLine()) != null) {
			String[] terms = line.split("\\s+");
			
			//First round
			if (previousCountry.equals("")) {
				countries++;
				previousCountry = terms[terms.length - 1];
			}
			
			if (terms[terms.length - 1].equals(previousCountry)) {
				if(terms.length > 4){
					StringBuilder b = new StringBuilder();
					b.append(terms[0]);
					b.append(" ");
					b.append(terms[1]);
					b.append(" ");
					for(int i = 2; i < terms.length - 2; i++){
						b.append(terms[i]);
						b.append("_");
					}
					b.append(terms[terms.length - 2]);
					b.append(" ");
					b.append(terms[terms.length - 1]);
					
					cities.add(b.toString());
				} else {
					cities.add(line);
				}
			} else {
				System.out.printf("Going to write country(%s) number %d to a new file called %s\n", previousCountry, countries, String.format("%s%s", output, previousCountry));
				
				//Writing to file
				File file = new File(output + previousCountry);
				BufferedWriter bw = new BufferedWriter(new FileWriter(file));
				Iterator<String> cityIterator = cities.iterator();
				while (cityIterator.hasNext()) {
					bw.write(String.format("%s\n", cityIterator.next()));
				}
				bw.close();
				
				//Keeping track
				cities.clear();
				previousCountry = terms[terms.length - 1];
				if(terms.length > 4){
					StringBuilder b = new StringBuilder();
					b.append(terms[0]);
					b.append(" ");
					b.append(terms[1]);
					b.append(" ");
					for(int i = 2; i < terms.length - 3; i++){
						b.append(terms[i]);
						b.append("_");
					}
					b.append(terms[terms.length - 2]);
					b.append(" ");
					b.append(terms[terms.length - 1]);
					
					cities.add(b.toString());
				} else {
					cities.add(line);
				}
				countries++;
			}
		}
		br.close();
	}
	
	public void run(String[] args) throws IOException {
		if (args.length < 2) {
			System.err.println("Error: to few arguments given");
			System.err.println("Usages: ParseCities [input] [output] [optionel: output to one file]");
			System.exit(0);
		}
		
		if(args.length > 2 && args[2].equals("oneFile")){
			toOneFile(args[0], args[1]);
		} else {
			toMultipleFiles(args[0], args[1]);
		}
	}
	
	public static void main(String[] args) {
		for (int i = 0; i < args.length; i++) {
			System.out.printf("%s ", args[i]);
		}
		try {
			new ParseCities().run(args);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
