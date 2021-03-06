package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * An N dimensional object to store a point
 *  from an input matrix for k-means clustering
 * @author stevenb
 * @date 02-09-2013
 */
public class Point implements WritableComparable<Point> {
	
	private double[] coordinates;
	private String[] nonNumericalValues; // Will contain non-numerical information of this point of concern in the data set, like data point names or racial information etc. 
	
	public Point(double[] coordinates, String[] nonNumericalValues) {
		this.coordinates = Arrays.copyOf(coordinates, coordinates.length);
		this.nonNumericalValues = Arrays.copyOf(nonNumericalValues, nonNumericalValues.length);
	}
	
	public Point(Point p) {
		coordinates = Arrays.copyOf(p.getCoordinates(), p.getCoordinates().length);
		nonNumericalValues = Arrays.copyOf(p.getNonNumericalValues(), p.getNonNumericalValues().length);
	}
	
	public Point(String coordinatesString) {
		nonNumericalValues = new String[0];
		setCoordinates(coordinatesString);
	}
	
	public Point() {
		this("");
	}
	
	/**
	 * Retrieve the coordinates of this Point
	 * @return the coordinates as a primitive array
	 */
	public double[] getCoordinates() {
		return coordinates;
	}
	
	/**
	 * Retrieve the coordinates of this Point
	 * @return the coordinates as a DoubleWritable array
	 */
	public DoubleWritable[] getCoordinatesWritable() {
		DoubleWritable[] writableCoordinates = new DoubleWritable[coordinates.length];
		for (int i = 0; i < coordinates.length; i++) {
			writableCoordinates[i] = new DoubleWritable(coordinates[i]);
		}
		return writableCoordinates;
	}
	
	/**
	 * Retrieve the nonNumericalValues variables of this Point
	 * @return a String array object containing
	 *  nonNumericalValues information
	 */
	public String[] getNonNumericalValues() {
		return nonNumericalValues;
	}
	
	/**
	 * Retrieve the non numerical Values of this Point
	 * @return the coordinates as a Text array
	 */
	public Text[] getNonNumericalValuesWritable() {
		Text[] writableNonNumericalValues = new Text[nonNumericalValues.length];
		for (int i = 0; i < nonNumericalValues.length; i++) {
			writableNonNumericalValues[i] = new Text(nonNumericalValues[i]);
		}
		return writableNonNumericalValues;
	}
	
	/**
	 * Set the coordinates of this point
	 * @param coordinates: double array object
	 * 	containing the coordinates
	 */
	public void setCoordinates(double[] coordinates) {
		this.coordinates = Arrays.copyOf(coordinates, coordinates.length);
	}
	
	/**
	 * Set the coordinates of this point
	 * First, create a double array which could fit
	 *  all coordinates.
	 * Fill as expected, but fill 'nonNumericalValues' if a Number
	 *  Format Exception occurs.
	 * After the string array is depleted, check if
	 *  nonNumericalValues had any objects. 
	 * If false, copy the newCoordinates to coordinates 
	 *  directly. If true, only copy the filled double
	 *  objects in newCoordinates to coordinates.
	 * @param coordinates: String object containing
	 * 	the coordinates
	 */
	public void setCoordinates(String coordinatesString) {
		String[] values = coordinatesString.split("\\s+");
		double[] newCoordinates = new double[values.length];
		for (int i = 0; i < values.length; i++) {
			try {
				newCoordinates[i - nonNumericalValues.length] = Double.parseDouble(values[i]);
			} catch (NumberFormatException e) {
				String[] newNonNumericalValues = new String[nonNumericalValues.length + 1];
				for (int j = 0; j < nonNumericalValues.length; j++) {
					newNonNumericalValues[j] = nonNumericalValues[j];
				}
				newNonNumericalValues[nonNumericalValues.length] = values[i];
				nonNumericalValues = newNonNumericalValues;
			}
		}
		
		coordinates = new double[values.length - nonNumericalValues.length];
		for (int i = 0; i < coordinates.length; i++) {
			coordinates[i] = newCoordinates[i];
		}
	}
	
	/**
	 * Set the coordinates of this point
	 * @param coordinates: String array object
	 * 	containing nonNumericalValues information
	 */
	public void setNonNumericalValues(String[] nonNumericalValues) {
		this.nonNumericalValues = nonNumericalValues;
	}
	
	/**
	 * Adds a point to this point
	 * @param p: the point to add to this point
	 */
	public void add(Point p) {
		double[] otherCoordinates = p.getCoordinates();
		for (int i = 0; i < coordinates.length; i++) {
			coordinates[i] += otherCoordinates[i];
		}
	}
	
	/**
	 * Divides this point by x
	 * @param x: the value to divide the point through
	 */
	public void divide(int x) {
		for (int i = 0; i < coordinates.length; i++) {
			coordinates[i] /= x;
		}
	}
	
	/**
	 * Calculate the Euclidean distance between this point
	 *  and the point 'p'
	 * @param p: the Point object to calculate the distance to
	 * @return: the Euclidean distance between this point and point p
	 */
	public double calculateDistance(Point p) {
		if (p == null) { // In case a non existing point is returned, we don't need to calculate this distance
			return Double.MAX_VALUE;
		}
		
		double dist = 0.0, distSum = 0.0;
		double pointCoordinates[] = p.getCoordinates();
		for (int i = 0; i < pointCoordinates.length; i++) {
			dist = pointCoordinates[i] - coordinates[i];
			distSum += Math.pow(dist, 2);
		}
		
		return Math.sqrt(distSum);
	}
	
	/**
	 * Check whether this is a zero
	 *  dimensions Point.
	 * @return: true if is empty and false if is filled
	 */
	public boolean isEmpty() {
		return coordinates.length <= 0;
	}
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		for (double coordinate : coordinates) {
			b.append(" ");
			b.append(coordinate);
		}
		for (String nonNumericalValue : nonNumericalValues) {
			b.append(" ");
			b.append(nonNumericalValue);
		}
		return b.toString();
	}
	
	public String nonNumericalValuesToString() {
		StringBuilder b = new StringBuilder();
		for (String nonNumericalValue : nonNumericalValues) {
			b.append(" ");
			b.append(nonNumericalValue);
		}
		return b.toString();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(coordinates.length);
		out.writeInt(nonNumericalValues.length);
		for (int i = 0; i < coordinates.length; i++) {
			out.writeDouble(coordinates[i]);
		}
		for (int i = 0; i < nonNumericalValues.length; i++) {
			Text t = new Text(nonNumericalValues[i]);
			t.write(out);
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int numCoordinates = in.readInt(), numNonNumericalValues = in.readInt();
		coordinates = new double[numCoordinates];
		for (int i = 0; i < numCoordinates; i++) {
			coordinates[i] = in.readDouble();
		}
		nonNumericalValues = new String[numNonNumericalValues];
		for (int i = 0; i < numNonNumericalValues; i++) {
			Text t = new Text();
			t.readFields(in);
			nonNumericalValues[i] = t.toString();
		}
	}
	
	@Override
	public int compareTo(Point point) {
		double[] otherCoordinates = point.getCoordinates();
		for (int i = 0; i < coordinates.length; i++) {
			double cmp = coordinates[i] - otherCoordinates[i];
			if (cmp != 0.0) {
				return (int) cmp;
			}
		}
		String[] otherNonNumericalValues = point.getNonNumericalValues();
		for (int i = 0; i < nonNumericalValues.length; i++) {
			int cmp = nonNumericalValues[i].compareTo(otherNonNumericalValues[i]);
			if (cmp != 0) {
				return cmp;
			}
		}
		return 0;
	}
	
	/**
	 * Compare a point to this point with a 
	 *  certain convergence value
	 * @param point: a Point object to compare this to
	 * @param convergencePoint: a float variable denoting the
	 * 	convergence point
	 * @return -0 if this is below point, 0 if this is equal to point
	 *  and +0 if this is above point.
	 */
	public double compareTo(Point point, float convergencePoint) {
		double[] otherCoordinates = point.getCoordinates();
		for (int i = 0; i < coordinates.length; i++) {
			double cmp = Math.abs(coordinates[i] - otherCoordinates[i]);
			if (cmp > convergencePoint) {
				return cmp;
			}
		}
		return 0;
	}
}
