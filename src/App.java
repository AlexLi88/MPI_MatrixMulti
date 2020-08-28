import mpi.*;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.lang.Math;
import java.lang.Integer;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class App {

    public static int[] readCSVFile(String fileName) {
        File file = new File(fileName);
        List<List<Integer>> data = new ArrayList<>();
        Scanner inputStream;
        try {
            inputStream = new Scanner(file);
            // Skip header line
            // inputStream.nextLine();
            while (inputStream.hasNext()) {
                String line = inputStream.next();
                String[] arr = line.split(",");
                int m = arr.length;
                List<Integer> rows = new ArrayList<>();
                for (int i = 0; i < m; i++) {
                    // Trim UTF8 BOM character,
                    // https://stackoverflow.com/questions/4897876/reading-utf-8-bom-marker
                    rows.add(Integer.parseInt(arr[i].replace("\uFEFF", "")));
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
        int[][] output = data.stream().map(u -> u.stream().mapToInt(i -> i).toArray()).toArray(int[][]::new);
        int[] output1D = Stream.of(output).flatMapToInt(IntStream::of).toArray();
        // System.out.println(Arrays.toString(output1D));
        return output1D;
    }

    public static int[] matrixMultiply(int n, int[] a, int[] b, int[] c) {
        int i, j, k;
        for (i = 0; i < n; i++)
            for (j = 0; j < n; j++)
                for (k = 0; k < n; k++)
                    c[i * n + j] += a[i * n + k] * b[k * n + j];

        return c;
    }

    public static int[] setLoaclMatrix(int[] a, int myCoords[], int blockDim, int cols) {
        int count = 0;
        int[] local = new int[blockDim * blockDim];
        for (int r = myCoords[0] * blockDim; r < blockDim * (myCoords[0] + 1); r++) {
            for (int i = myCoords[1] * blockDim; i < (myCoords[1] + 1) * blockDim; i++) {
                int coor = i + r * cols;
                local[count] = a[i + r * cols];
                count++;
            }
        }
        return local;
    }

    public static int[][] reorganizeResults(int row, int n, int blockDim, int rank, int[][] result, int[] input) {
        int coodY = rank / n;
        int coodX = rank % n;

        for (int i = 0; i < input.length; i++) {
            int y = coodY * blockDim + i / blockDim;
            int x = coodX * blockDim + i % blockDim;
            result[y][x] = input[i];
        }

        return result;
    }

    public static int[] copyReceivToLocal(int[] receiveLocal, int[] local) {
        for (int i = 0; i < local.length; i++) {
            local[i] = receiveLocal[i];
        }
        return local;
    }

    public static String prettyPrint(int[][] input, int precision) {
        int rows = input.length;
        int cols = input[0].length;
        String res = "";
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                // BigDecimal b = new BigDecimal(input[i][j]);
                // double f = b.setScale(precision, RoundingMode.HALF_UP).doubleValue();
                res += String.valueOf(input[i][j]) + "\t";
            }
            res += "\n";
        }
        return res;
    }

    public static void writeResultToCSV(int[][] result, String filePath) throws IOException {
        BufferedWriter br = new BufferedWriter(new FileWriter(filePath));
        StringBuilder sb = new StringBuilder();
        for (int[] arr : result) {
            for (int element : arr) {
                sb.append(String.valueOf(element));
                sb.append(",");
            }
            sb.append("\n");
        }

        br.write(sb.toString());
        br.close();
    }

    public static void exchangeSubblock(Cartcomm comm2D, int my2DRank, ShiftParms shiftTarget, int[] local,
            int[] receivLocal, int blockDim) throws MPIException {
        if (shiftTarget.rank_source != my2DRank) {
            // System.out.println("My 2d rank is: " + my2DRank + " Rank L Source: " +
            // shiftTarget.rank_source
            // + " Rank L Dest: " + shiftTarget.rank_dest);
            if (my2DRank < shiftTarget.rank_dest) {
                comm2D.Send(local, 0, blockDim * blockDim, MPI.INT, shiftTarget.rank_source, my2DRank);
                comm2D.Recv(receivLocal, 0, blockDim * blockDim, MPI.INT, shiftTarget.rank_source,
                        shiftTarget.rank_source);
            } else {
                comm2D.Recv(receivLocal, 0, blockDim * blockDim, MPI.INT, shiftTarget.rank_source,
                        shiftTarget.rank_source);
                comm2D.Send(local, 0, blockDim * blockDim, MPI.INT, shiftTarget.rank_source, my2DRank);
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
        int[] globalA = new int[rows * rows];
        int[] globalB = new int[rows * rows];
        int blockDim = 0;
        int sqrootProcess = 0;
        int[] dims = new int[2];
        boolean[] periods = new boolean[2];
        Cartcomm comm2D;
        int my2DRank;
        int myCoords[] = new int[2];
        long startTime = 0;
        long endTime = 0;
        if (MPI.COMM_WORLD.Rank() == 0) {
            System.out.println("Number of Processes is " + nProcesses);
            startTime = System.currentTimeMillis();
            System.out.println("Start Timer: " + startTime);
            String fileNameA = args[0];
            String fileNameB = args[1];
            globalA = readCSVFile(fileNameA);
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
        MPI.COMM_WORLD.Bcast(globalA, 0, rows * rows, MPI.INT, 0);
        MPI.COMM_WORLD.Bcast(globalB, 0, rows * rows, MPI.INT, 0);
        int[] localA = new int[blockDim * blockDim];
        int[] localB = new int[blockDim * blockDim];
        int[] localC = new int[blockDim * blockDim];

        int[] receiveLocalA = new int[blockDim * blockDim];
        int[] receiveLocalB = new int[blockDim * blockDim];
        dims[0] = sqrootProcess;
        dims[1] = sqrootProcess;
        periods[0] = true;
        periods[1] = true;
        CartParms topoParams;
        ShiftParms leftShift, rightShift, upShift, downShift;

        comm2D = MPI.COMM_WORLD.Create_cart(dims, periods, false);
        my2DRank = comm2D.Rank();
        myCoords = comm2D.Coords(my2DRank);

        System.out.println("My 2d rank is: " + my2DRank);
        // System.out.println("My rank is: " + myRank + Arrays.toString(brocastData));
        System.out.println("My Coords is: " + my2DRank + " " + Arrays.toString(myCoords));
        localA = setLoaclMatrix(globalA, myCoords, blockDim, cols);
        localB = setLoaclMatrix(globalB, myCoords, blockDim, cols);
        // System.out.println("My rank isA: " + myRank + " " + Arrays.toString(localA));
        // System.out.println("My rank isB: " + myRank + " " + Arrays.toString(localB));
        leftShift = comm2D.Shift(1, myCoords[0]);
        upShift = comm2D.Shift(0, myCoords[1]);
        exchangeSubblock(comm2D, my2DRank, leftShift, localA, receiveLocalA, blockDim);
        exchangeSubblock(comm2D, my2DRank, upShift, localB, receiveLocalB, blockDim);
        comm2D.Barrier();

        for (int i = 0; i < dims[0]; i++) {
            matrixMultiply(blockDim, localA, localB, localC);
            leftShift = comm2D.Shift(1, 1);
            upShift = comm2D.Shift(0, 1);
            exchangeSubblock(comm2D, my2DRank, leftShift, localA, receiveLocalA, blockDim);
            exchangeSubblock(comm2D, my2DRank, upShift, localB, receiveLocalB, blockDim);
            comm2D.Barrier();
        }

        if (my2DRank == 0) {
            int[][] finalC = new int[rows][cols];
            finalC = reorganizeResults(rows, dims[0], blockDim, 0, finalC, localC);
            int[] tmpC = new int[blockDim * blockDim];
            for (int r = 1; r < nProcesses; r++) {
                comm2D.Recv(tmpC, 0, blockDim * blockDim, MPI.INT, r, r);
                finalC = reorganizeResults(rows, dims[0], blockDim, r, finalC, tmpC);
            }
            endTime = System.currentTimeMillis();
            System.out.println("Stop Timer: " + endTime);
            System.out.println("Elapsed time is: " + (endTime - startTime));
            // System.out.println(prettyPrint(finalC, 2));
            try {
                writeResultToCSV(finalC, "output.csv");
            } catch (Exception e) {
                System.out.println("IO EXCEPTION");
            }
        } else {
            comm2D.Send(localC, 0, blockDim * blockDim, MPI.INT, 0, my2DRank);
        }
        MPI.Finalize();
    }
}
