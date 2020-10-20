import mpi.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import java.lang.Math;
import java.math.BigDecimal;
import java.math.RoundingMode;

// mpirun -n 4 java App 2048_2048_int.csv 2048_2048_int.csv 2048
// mpirun -n 4 java App 10_10.csv 10_10.csv 10
public class App {

    public static double[] readCSVFile(String fileName) {
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
        double[] output1D = Stream.of(output).flatMapToDouble(DoubleStream::of).toArray();
        // System.out.println(Arrays.toString(output1D));
        return output1D;
    }

    public static double[] matrixMultiply(int n, double[] a, double[] b, double[] c) {
        int i, j, k;
        for (i = 0; i < n; i++)
            for (j = 0; j < n; j++)
                for (k = 0; k < n; k++)
                    c[i * n + j] += a[i * n + k] * b[k * n + j];

        return c;
    }

    public static double[] setLoaclMatrix(double[] a, int myCoords[], int blockDim, int cols) {
        int count = 0;
        double[] local = new double[blockDim * blockDim];
        for (int r = myCoords[0] * blockDim; r < blockDim * (myCoords[0] + 1); r++) {
            for (int i = myCoords[1] * blockDim; i < (myCoords[1] + 1) * blockDim; i++) {
                int coor = i + r * cols;
                local[count] = a[i + r * cols];
                count++;
            }
        }
        return local;
    }

    public static double[][] reorganizeResults(int row, int n, int blockDim, int rank, double[][] result,
            double[] input) {
        int coodY = rank / n;
        int coodX = rank % n;

        for (int i = 0; i < input.length; i++) {
            int y = coodY * blockDim + i / blockDim;
            int x = coodX * blockDim + i % blockDim;
            result[y][x] = input[i];
        }

        return result;
    }

    public static double[] copyReceivToLocal(double[] receiveLocal, double[] local) {
        for (int i = 0; i < local.length; i++) {
            local[i] = receiveLocal[i];
        }
        return local;
    }

    public static String prettyPrint(double[][] input, int precision) {
        int rows = input.length;
        int cols = input[0].length;
        String res = "";
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                BigDecimal b = new BigDecimal(input[i][j]);
                double f = b.setScale(precision, RoundingMode.HALF_UP).doubleValue();
                res += String.valueOf(f) + "\t";
            }
            res += "\n";
        }
        return res;
    }

    public static void exchangeSubblock(Cartcomm comm2D, int my2DRank, ShiftParms shiftTarget, double[] local,
            double[] receivLocal, int blockDim) throws MPIException {
        if (shiftTarget.rank_source != my2DRank) {
            // System.out.println("My 2d rank is: " + my2DRank + " Rank L Source: " +
            // shiftTarget.rank_source
            // + " Rank L Dest: " + shiftTarget.rank_dest);
            if (my2DRank < shiftTarget.rank_dest) {
                comm2D.Send(local, 0, blockDim * blockDim, MPI.DOUBLE, shiftTarget.rank_source, my2DRank);
                comm2D.Recv(receivLocal, 0, blockDim * blockDim, MPI.DOUBLE, shiftTarget.rank_source,
                        shiftTarget.rank_source);
            } else {
                comm2D.Recv(receivLocal, 0, blockDim * blockDim, MPI.DOUBLE, shiftTarget.rank_source,
                        shiftTarget.rank_source);
                comm2D.Send(local, 0, blockDim * blockDim, MPI.DOUBLE, shiftTarget.rank_source, my2DRank);
            }
            local = copyReceivToLocal(receivLocal, local);
        }
    }

    public static void main(String[] args) throws MPIException {
        if (args.length != 3) {
            System.err.println("Usage: <fileA> <fileB> <size>");
            System.exit(1);
        }
        MPI.Init(args);
        int nProcesses = MPI.COMM_WORLD.Size();
        int myRank = MPI.COMM_WORLD.Rank();
        int[] brocastData = new int[4];
        int rows = Integer.parseInt(args[2]);
        int cols = rows;
        double[] globalA = new double[rows * rows];
        double[] globalB = new double[rows * rows];
        int blockDim = 0;
        int sqrootProcess = 0;
        int[] dims = new int[2];
        boolean[] periods = new boolean[2];
        Cartcomm comm2D;
        int my2DRank;
        int myCoords[] = new int[2];
        long startTime = 0;
        long fileReadTime = 0;
        long endTime = 0;
        if (MPI.COMM_WORLD.Rank() == 0) {
            System.out.println("Number of Processes is " + nProcesses);
            startTime = System.currentTimeMillis();
            System.out.println("Start Timer: " + startTime);
            String fileNameA = args[0];
            String fileNameB = args[1];
            globalA = readCSVFile(fileNameA);
            fileReadTime = System.currentTimeMillis();
            System.out.println("Reading CSV file time: " + (fileReadTime - startTime));
            globalB = readCSVFile(fileNameB);
            rows = (int) Math.sqrt(globalA.length);
            cols = rows;
            blockDim = 0;
            System.out.println("Rows: " + rows + " Cols: " + cols);
            double sqrootNp = Math.sqrt(nProcesses);
            if (sqrootNp != Math.floor(sqrootNp)) {
                System.out.println("ERROR: The number of processes must be a perfect square");
                MPI.COMM_WORLD.Abort(1);
            }

            sqrootProcess = (int) sqrootNp;
            if (rows % sqrootProcess != 0 || cols % sqrootProcess != 0) {
                System.out.println(
                        String.format("ERROR: nuber of rows and cols can not be devided by %d", sqrootProcess));
                MPI.COMM_WORLD.Abort(1);
            }
            blockDim = cols / sqrootProcess;
            System.out.println("Square root of nProcesses is : " + sqrootProcess);
            System.out.println("Block Dims is: " + blockDim);
            brocastData[0] = rows;
            brocastData[1] = cols;
            brocastData[2] = sqrootProcess;
            brocastData[3] = blockDim;

        }
        MPI.COMM_WORLD.Bcast(brocastData, 0, 4, MPI.INT, 0);
        rows = brocastData[0];
        cols = brocastData[1];
        sqrootProcess = brocastData[2];
        blockDim = brocastData[3];
        MPI.COMM_WORLD.Bcast(globalA, 0, rows * rows, MPI.DOUBLE, 0);
        MPI.COMM_WORLD.Bcast(globalB, 0, rows * rows, MPI.DOUBLE, 0);
        double[] localA = new double[blockDim * blockDim];
        double[] localB = new double[blockDim * blockDim];
        double[] localC = new double[blockDim * blockDim];

        double[] receivLocalA = new double[blockDim * blockDim];
        double[] receivLocalB = new double[blockDim * blockDim];

        dims[0] = sqrootProcess;
        dims[1] = sqrootProcess;
        periods[0] = true;
        periods[1] = true;
        CartParms topoParams;
        ShiftParms leftShift, rightShift, upShift, downShift;

        comm2D = MPI.COMM_WORLD.Create_cart(dims, periods, false);
        my2DRank = comm2D.Rank();
        myCoords = comm2D.Coords(my2DRank);

        // System.out.println("My 2d rank is: " + my2DRank);
        // System.out.println("My rank is: " + myRank + Arrays.toString(brocastData));
        // System.out.println("My Coords is: " + my2DRank + " " +
        // Arrays.toString(myCoords));
        localA = setLoaclMatrix(globalA, myCoords, blockDim, cols);
        localB = setLoaclMatrix(globalB, myCoords, blockDim, cols);
        // System.out.println("My rank isA: " + myRank + " " + Arrays.toString(localA));
        // System.out.println("My rank isB: " + myRank + " " + Arrays.toString(localB));
        leftShift = comm2D.Shift(1, myCoords[0]);
        upShift = comm2D.Shift(0, myCoords[1]);

        exchangeSubblock(comm2D, my2DRank, leftShift, localA, receivLocalA, blockDim);
        exchangeSubblock(comm2D, my2DRank, upShift, localB, receivLocalB, blockDim);

        comm2D.Barrier();

        for (int i = 0; i < dims[0]; i++) {
            matrixMultiply(blockDim, localA, localB, localC);
            leftShift = comm2D.Shift(1, 1);
            upShift = comm2D.Shift(0, 1);
            exchangeSubblock(comm2D, my2DRank, leftShift, localA, receivLocalA, blockDim);
            exchangeSubblock(comm2D, my2DRank, upShift, localB, receivLocalB, blockDim);
            comm2D.Barrier();
        }
        if (my2DRank == 0) {
            double[][] finalC = new double[rows][cols];
            finalC = reorganizeResults(rows, dims[0], blockDim, 0, finalC, localC);
            double[] tmpC = new double[blockDim * blockDim];
            for (int r = 1; r < nProcesses; r++) {
                comm2D.Recv(tmpC, 0, blockDim * blockDim, MPI.DOUBLE, r, r);
                finalC = reorganizeResults(rows, dims[0], blockDim, r, finalC, tmpC);
            }
            endTime = System.currentTimeMillis();
            System.out.println("Stop Timer: " + endTime);
            System.out.println("Elapsed time is: " + (endTime - startTime));

            // System.out.println(prettyPrint(finalC, 2));
        } else {
            comm2D.Send(localC, 0, blockDim * blockDim, MPI.DOUBLE, 0, my2DRank);
        }
        MPI.Finalize();
    }
}
