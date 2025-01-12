package org.ulpgc;


import java.io.Serializable;
import java.util.concurrent.Callable;

public class MatrixBlockTask implements Callable<int[][]>, Serializable {
    final int[][] blockA;
    final int[][] blockB;
    final int chunkSize;
    private final String algorithm;
    private static final int NAIVE_THRESHOLD = 128;

    public MatrixBlockTask(int[][] blockA, int[][] blockB, int chunkSize, String algorithm) {
        this.blockA = blockA;
        this.blockB = blockB;
        this.chunkSize = chunkSize;
        this.algorithm = algorithm;
    }

    @Override
    public int[][] call() {
        if ("Strassen".equalsIgnoreCase(algorithm)) {
            return calculateUsingStrassen(blockA, blockB);
        } else {
            return calculateUsingNaive(blockA, blockB);
        }
    }

    private int[][] calculateUsingNaive(int[][] a, int[][] b) {
        int dimension = a.length;
        int[][] result = new int[dimension][dimension];

        for (int i = 0; i < dimension; i++) {
            for (int j = 0; j < dimension; j++) {
                for (int k = 0; k < dimension; k++) {
                    result[i][j] += a[i][k] * b[k][j];
                }
            }
        }

        return result;
    }

    private int[][] calculateUsingStrassen(int[][] a, int[][] b) {
        int n = a.length;
        if (n <= NAIVE_THRESHOLD) {
            return calculateUsingNaive(a, b);
        }

        int newSize = n / 2;
        int[][] a11 = new int[newSize][newSize];
        int[][] a12 = new int[newSize][newSize];
        int[][] a21 = new int[newSize][newSize];
        int[][] a22 = new int[newSize][newSize];
        int[][] b11 = new int[newSize][newSize];
        int[][] b12 = new int[newSize][newSize];
        int[][] b21 = new int[newSize][newSize];
        int[][] b22 = new int[newSize][newSize];

        for (int i = 0; i < newSize; i++) {
            for (int j = 0; j < newSize; j++) {
                a11[i][j] = a[i][j];
                a12[i][j] = a[i][j + newSize];
                a21[i][j] = a[i + newSize][j];
                a22[i][j] = a[i + newSize][j + newSize];

                b11[i][j] = b[i][j];
                b12[i][j] = b[i][j + newSize];
                b21[i][j] = b[i + newSize][j];
                b22[i][j] = b[i + newSize][j + newSize];
            }
        }

        int[][] m1 = calculateUsingStrassen(add(a11, a22), add(b11, b22));
        int[][] m2 = calculateUsingStrassen(add(a21, a22), b11);
        int[][] m3 = calculateUsingStrassen(a11, subtract(b12, b22));
        int[][] m4 = calculateUsingStrassen(a22, subtract(b21, b11));
        int[][] m5 = calculateUsingStrassen(add(a11, a12), b22);
        int[][] m6 = calculateUsingStrassen(subtract(a21, a11), add(b11, b12));
        int[][] m7 = calculateUsingStrassen(subtract(a12, a22), add(b21, b22));

        int[][] c11 = add(subtract(add(m1, m4), m5), m7);
        int[][] c12 = add(m3, m5);
        int[][] c21 = add(m2, m4);
        int[][] c22 = add(subtract(add(m1, m3), m2), m6);

        int[][] result = new int[n][n];

        for (int i = 0; i < newSize; i++) {
            for (int j = 0; j < newSize; j++) {
                result[i][j] = c11[i][j];
                result[i][j + newSize] = c12[i][j];
                result[i + newSize][j] = c21[i][j];
                result[i + newSize][j + newSize] = c22[i][j];
            }
        }

        return result;
    }

    private int[][] add(int[][] x, int[][] y) {
        int n = x.length;
        int[][] result = new int[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                result[i][j] = x[i][j] + y[i][j];
            }
        }
        return result;
    }

    private int[][] subtract(int[][] x, int[][] y) {
        int n = x.length;
        int[][] result = new int[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                result[i][j] = x[i][j] - y[i][j];
            }
        }
        return result;
    }
}
