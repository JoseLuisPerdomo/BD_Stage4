package org.ulpgc;

import java.util.Random;

public class MatrixGenerator {
    public static int[][] createMatrix(int rows, int columns) {
        Random rng = new Random();
        int[][] matrix = new int[rows][columns];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                matrix[i][j] = rng.nextInt(100) + 1;
            }
        }
        return matrix;
    }
}
