import java.util.Arrays;
import java.math.BigDecimal;
import java.math.RoundingMode;

class Matrix {
    private int rows;
    private int cols;
    private double data[][];

    public Matrix(int rows, int columns) {
        this.rows = rows;
        this.cols = columns;
        this.data = new double[rows][columns];
    }

    public int getRows() {
        return this.rows;
    }

    public int getCols() {
        return this.cols;
    }

    public double[][] getData() {
        return this.data;
    }

    public void setData(double[][] input) {
        if (input.length != this.rows || input[0].length != this.cols) {
            throw new RuntimeException("The input matrix size is different than the original matrx size ");
        }
        for (int i = 0; i < input.length; i++) {
            for (int j = 0; j < input[0].length; j++) {
                this.data[i][j] = input[i][j];
            }
        }
    }

    public Matrix transpose() {
        double[][] temp = new double[this.cols][this.rows];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < this.cols; j++) {
                temp[j][i] = data[i][j];
            }
        }
        Matrix result = new Matrix(cols, rows);
        result.setData(temp);
        return result;
    }

    public Matrix dot(double[][] m2) {
        if (this.cols != m2.length) {
            throw new RuntimeException("Size of the first matrix and second matrix are not aligned");
        }
        double[][] product = new double[this.rows][m2[0].length];
        int r = product.length;
        int c = product[0].length;
        for (int i = 0; i < r; i++) {
            for (int j = 0; j < c; j++) {
                double tmp = 0;
                for (int k = 0; k < m2.length; k++) {
                    tmp += this.data[i][k] * m2[k][j];
                }
                product[i][j] = tmp;
            }
        }
        Matrix result = new Matrix(r, c);
        result.setData(product);
        return result;
    }

    public double[][] norm(int axis) {
        double[][] result;
        if (axis == 0) {
            result = new double[1][this.cols];
            for (int i = 0; i < this.cols; i++) {
                double tmp = 0;
                for (int j = 0; j < this.rows; j++) {
                    tmp = tmp += Math.pow(this.data[j][i], 2);
                }
                result[0][i] = Math.sqrt(tmp);
            }
        } else if (axis == 1) {
            result = new double[this.rows][1];
        } else {
            throw new RuntimeException("The axis has to be either 0 or 1");
        }
        return result;
    }

    public void divide(double[][] matrix) {
        for (int i = 0; i < this.rows; i++) {
            for (int j = 0; j < this.cols; j++) {
                this.data[i][j] = this.data[i][j] / matrix[0][j];
            }
        }
    }

    public String toString(int precision) {
        String res = "";
        for (int i = 0; i < this.rows; i++) {
            for (int j = 0; j < this.cols; j++) {
                BigDecimal b = new BigDecimal(this.data[i][j]);
                double f = b.setScale(precision, RoundingMode.HALF_UP).doubleValue();
                res += String.valueOf(f) + "\t";
            }
            res += "\n";
        }
        return res;
    }
}
