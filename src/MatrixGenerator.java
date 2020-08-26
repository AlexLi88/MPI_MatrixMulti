import java.io.*;
import java.util.*;

public class MatrixGenerator {
    private static final char DEFAULT_SEPARATOR = ',';

    public static List<List<Double>> getRandomData(int r, int c) {
        List<List<Double>> data = new ArrayList<>();
        // double[][] data = new double[r][c];
        // create random object
        Random random = new Random();
        for (int i = 0; i < r; i++) {
            data.add(new ArrayList<>());
            for (int j = 0; j < c; j++) {
                double rn = random.nextDouble();
                data.get(i).add(rn);
            }
        }
        return data;
    }

    public static List<Integer> getRandomDataByRow(int c) {
        List<Integer> data = new LinkedList<>();
        Random random = new Random();
        for (int j = 0; j < c; j++) {
            int rn = random.nextInt(100);
            data.add(rn);
        }

        return data;
    }

    public static void writeLine(Writer w, List<Integer> values) throws IOException {
        writeLine(w, values, DEFAULT_SEPARATOR, ' ');
    }

    public static void writeLine(Writer w, List<Integer> values, char separators) throws IOException {
        writeLine(w, values, separators, ' ');
    }

    // https://tools.ietf.org/html/rfc4180
    private static String followCVSformat(String value) {

        String result = value;
        if (result.contains("\"")) {
            result = result.replace("\"", "\"\"");
        }
        return result;

    }

    public static void writeLine(Writer w, List<Integer> values, char separators, char customQuote) throws IOException {

        boolean first = true;

        // default customQuote is empty

        if (separators == ' ') {
            separators = DEFAULT_SEPARATOR;
        }

        StringBuilder sb = new StringBuilder();
        for (int v : values) {
            String value = String.valueOf(v);
            if (!first) {
                sb.append(separators);
            }
            if (customQuote == ' ') {
                sb.append(followCVSformat(value));
            } else {
                sb.append(customQuote).append(followCVSformat(value)).append(customQuote);
            }

            first = false;
        }
        sb.append("\n");
        w.append(sb.toString());
    }

    public static void writeDataToCSVFile(int row, int cols, String filePath) throws Exception {
        System.out.println("Generating CSV file: " + filePath);

        String csvFile = filePath;
        FileWriter writer = new FileWriter(csvFile);
        for (int i = 0; i < row; i++) {
            if (i % 100 == 0) {
                System.out.println("Generating row: " + i);
            }
            List<Integer> value = MatrixGenerator.getRandomDataByRow(cols);
            MatrixGenerator.writeLine(writer, value);
        }
        // for (List<Double> value : data) {
        // RandomNumber.writeLine(writer, value);
        // }
        writer.flush();
        writer.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MatrixGenerator <Row Size> <Column Size> <File Path>");
            System.exit(1);
        }

        int row = Integer.parseInt(args[0]);
        int col = Integer.parseInt(args[1]);
        String filePath = args[2];

        writeDataToCSVFile(row, col, String.format("./%s", filePath));
    }
}