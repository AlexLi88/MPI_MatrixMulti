import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;

public class MatrixMulti {

    public static double[][] readCSVFile(String fileName) {
        File file = new File(fileName);
        List<List<Double>> data = new ArrayList<>();
        Scanner inputStream;
        try {
            inputStream = new Scanner(file);
            // Skip header line
            // inputStream.nextLine();
            while (inputStream.hasNext()) {
                String line = inputStream.next();
                String[] arr = line.split(",");
                int m = arr.length;
                List<Double> rows = new ArrayList<>();
                for (int i = 0; i < m; i++) {
                    // Trim UTF8 BOM character,
                    // https://stackoverflow.com/questions/4897876/reading-utf-8-bom-marker
                    rows.add(Double.parseDouble(arr[i].replace("\uFEFF", "")));
                }
                // this adds the currently parsed line to the 2-dimensional string array
                data.add(rows);
            }
            inputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        // Convert ArrayList into double[][] array,
        // source:
        // https://stackoverflow.com/questions/10043209/convert-arraylist-into-2d-array-containing-varying-lengths-of-arrays
        double[][] output = data.stream().map(u -> u.stream().mapToDouble(i -> i).toArray()).toArray(double[][]::new);
        // double[] output1D =
        // Stream.of(output).flatMapToDouble(DoubleStream::of).toArray();
        // System.out.println(Arrays.toString(output1D));
        return output;
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        String fileNameA = args[0];
        String fileNameB = args[1];
        double[][] a = readCSVFile(fileNameA);
        long fileReadTime = System.currentTimeMillis();
        System.out.println("Reading CSV file time: " + (fileReadTime - startTime));
        double[][] b = readCSVFile(fileNameB);
        Matrix ma = new Matrix(a.length, a[0].length);
        Matrix mb = new Matrix(b.length, a[0].length);
        ma.setData(a);
        mb.setData(b);
        Matrix mc = ma.dot(mb.getData());
        long calculationTime = System.currentTimeMillis();
        System.out.println("Calculation time: " + (calculationTime - fileReadTime));
        System.out.println("Total time: " + (calculationTime - startTime));
    }
}
