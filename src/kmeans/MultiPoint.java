package kmeans;

/*
    kmeans.MultiPoint class denotes a single university in an N dimensional space.
    Every university attribute is converted to a double value and thus a
    university is represented as a array of double values or "coordinates"

 */
public class MultiPoint {

    private String universityName;

    // the attributes presented as an array
    private double[] coordinates;


    public MultiPoint(String universityName, double[] coordinates) {

        this.universityName = universityName;
        this.coordinates = new double[coordinates.length];
        System.arraycopy(coordinates, 0, this.coordinates, 0, coordinates.length);
    }

    public String getUniversityName() {
        return this.universityName;
    }

    public double[] getCoordinates() {
        return this.coordinates;
    }

    @Override
    public String toString() {
        StringBuilder output = new StringBuilder(this.universityName);

        for(double coordinate : coordinates){
            output.append(",").append(Double.toString(coordinate));
        }

        return output.toString();
    }

    //calculates distance between 2 multi points
    public double distance(MultiPoint point) {

        double sum = 0;
        for (int i = 0; i < this.getCoordinates().length; i++) {
            sum += Math.pow((this.getCoordinates()[i] - point.getCoordinates()[i]), 2.0);
        }

        return Math.sqrt(sum);
    }
}