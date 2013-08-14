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
	
	public void run(String[] args) throws IOException {
		int countries = 0;
		String input = "", output = "", line = "", previousCountry = "";
		if (args.length < 2) {
			System.err.println("Error: to few arguments given");
			System.exit(0);
		}
		input = args[0];
		output = args[1];
		
		BufferedReader br = new BufferedReader(new FileReader(input));
		while ((line = br.readLine()) != null) {
			String[] terms = line.split("\\s+");
			
			//First round
			if (previousCountry.equals("")) {
				countries++;
				previousCountry = terms[terms.length - 1];
			}
			
			if (terms[terms.length - 1].equals(previousCountry)) {
				cities.add(line);
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
				countries++;
			}
		}
		br.close();
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
