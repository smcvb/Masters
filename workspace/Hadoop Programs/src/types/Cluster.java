package types;

/**
 * A object to hold a cluster for
 *  the k-means algorithm
 * @author stevenb
 * @date 01-08-2013
 */
public class Cluster {
	
	private int index, size, dimensions;
	private Point centroid;
	private Point[] outliers;
	
	public Cluster(int index, int size, int dimensions, Point centroid, Point[] outliers) {
		this.index = index;
		this.size = size;
		this.dimensions = dimensions;
		this.centroid = centroid;
		this.outliers = outliers;
	}
	
	public Cluster() {
		this(-1, 0, 0, null, null);
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public int getSize() {
		return size;
	}
	
	public void setSize(int size) {
		this.size = size;
	}
	
	public int getDimensions() {
		return dimensions;
	}
	
	public void setDimensions(int dimensions) {
		this.dimensions = dimensions;
	}
	
	public Point getCentroid() {
		return centroid;
	}
	
	public void setCentroid(Point centroid) {
		this.centroid = centroid;
	}
	
	public Point[] getOutliers() {
		return outliers;
	}
	
	public void setOutliers(Point[] outliers) {
		this.outliers = outliers;
	}
	
	public void setCluster(int index, int size, int dimensions, Point centroid, Point[] outliers) {
		this.index = index;
		this.size = size;
		this.dimensions = dimensions;
		this.centroid = centroid;
		this.outliers = outliers;
	}
	
	public boolean isEmpty() {
		if (index == -1) {
			return true;
		}
		return false;
	}
	
	public void parseCluster(String clusterInfo, int kmeans) {
		String[] clusterInfoValues = clusterInfo.toString().replaceAll("\\s+", " ").split("\\s+");
		int start = 0, end = clusterInfoValues.length;
		outliers = new Point[kmeans - 1];
//		System.out.printf("CLUSTER: clusterInfo: %s\n", clusterInfo); // TODO REMOVE
//		System.out.printf("CLUSTER: Split line is %d long and has\n", clusterInfoValues.length); // TODO REMOVE
//		for (int j = 0; j < clusterInfoValues.length; j++) { // TODO REMOVE
//			//System.out.println("[" + clusterInfoValues[j] + "] - " + clusterInfoValues[j].length()); // TODO REMOVE
//			System.out.printf("\t%d = [%s]\n", j, clusterInfoValues[j]); // TODO REMOVE
//		} // TODO REMOVE
		
		try { // Parse index and size
			index = (int) Double.parseDouble(clusterInfoValues[0]); // Use parseDouble() to make '0' parsing valid
			size = (int) Double.parseDouble(clusterInfoValues[1]);
			dimensions = (int) Double.parseDouble(clusterInfoValues[2]);
		} catch (NumberFormatException e) {
			System.err.println("Error: expected an Integer instead of " + clusterInfoValues[0] + " " + clusterInfoValues[1]);
		}
//		System.out.printf("CLUSTER: Parsed index: %d, parsed size: %d and parsed dimensions: %d\n", index, size, dimensions); // TODO REMOVE
		
		if (isEmpty()) { // Empty cluster encountered, cannot parse mean and outliers
//			System.out.printf("CLUSTER: Found an empty cluster!\n"); // TODO REMOVE
			return;
		}
		
		// Parse cluster centroid
		start += 3;
		double[] coordinates = new double[dimensions];
		for (int i = start; i < start + dimensions; i++) {
			try {
				coordinates[i - 3] = Double.parseDouble(clusterInfoValues[i]);
			} catch (NumberFormatException e) {
//				System.err.println("Error: expected a Double instead of " + clusterInfoValues[i]);// TODO REMOVE
				System.err.println("Error Centroid: expected a Double instead of " + clusterInfoValues[i] + " at index " + i);// TODO REMOVE
			}
		}
		centroid = new Point(coordinates, new String[0]);
		//centroid.setCoordinates(coordinates);
//		System.out.printf("CLUSTER: Parsed centroid: ");// TODO REMOVE
//		for (int i = 0; i < coordinates.length; i++) {// TODO REMOVE
//			System.out.printf("%f ", coordinates[i]);// TODO REMOVE
//		}// TODO REMOVE
//		System.out.printf("\n"); // TODO REMOVE
		
		// Parse clusters k - 1 outliers
		for (int i = 0; i < kmeans - 1; i++) {
			double[] outlierCoordinates = new double[dimensions];
			start += dimensions;
//			System.out.printf("CLUSTER: start: %d and end: %d\n", start, end); // TODO REMOVE
			if (start == end) {
//				System.out.printf("CLUSTER: start and end equaled, hence will break from loop since there are no outliers\n"); // TODO REMOVE
				break;
			}
			for (int j = start; j < start + dimensions; j++) {
				try {
					outlierCoordinates[j - start] = Double.parseDouble(clusterInfoValues[j]);
				} catch (NumberFormatException e) {
//					System.err.println("Error: expected a Double instead of " + clusterInfoValues[i]);// TODO REMOVE
					System.err.println("Error Outlier: expected a Double instead of " + clusterInfoValues[i] + " at index " + i);// TODO REMOVE
				}
			}
			outliers[i] = new Point(outlierCoordinates, new String[0]);
			
//			System.out.printf("CLUSTER: Parsed outlier: ");// TODO REMOVE
//			for (int l = 0; l < outlierCoordinates.length; l++) {// TODO REMOVE
//				System.out.printf("%f ", outlierCoordinates[l]);// TODO REMOVE
//			}// TODO REMOVE
//			System.out.printf("\n"); // TODO REMOVE
			
		}
		
//		System.out.printf("CLUSTER: New Cluster:\n\t%s\n\n", toString()); // TODO REMOVE
	}
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		
		b.append(index);
		b.append(" ");
		b.append(size);
		b.append(" ");
		b.append(dimensions);
		
		if (centroid != null) {
			b.append("[");
			b.append(centroid.toString());
			b.append(" ] { ");
		}
		
		if (outliers != null) {
			for (int i = 0; i < outliers.length; i++) {
				if (outliers[i] != null) {
					b.append("[");
					b.append(outliers[i].toString());
					b.append(" ] ");
				} else {
					b.append("[ ] ");
				}
			}
		}
		b.append(" }");
		
		return b.toString();
	}
}
