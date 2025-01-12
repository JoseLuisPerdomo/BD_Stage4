package org.ulpgc;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.map.IMap;

import java.util.concurrent.*;

public class DistributedMatrixMultiplier {

    private static HazelcastInstance hazelcast;
    private static IMap<String, int[][]> partialResults;

    public static void main(String[] args) throws Exception {
        Config configuration = new Config();

        configuration.getSerializationConfig().addSerializerConfig(
                new SerializerConfig()
                        .setTypeClass(MatrixBlockTask.class)
                        .setImplementation(new MatrixBlockTaskSerializer())
        );

        NetworkConfig netConfig = configuration.getNetworkConfig();
        netConfig.getJoin().getMulticastConfig().setEnabled(false);
        netConfig.getJoin().getTcpIpConfig().setEnabled(true).addMember("192.168.1.22");

        hazelcast = Hazelcast.newHazelcastInstance(configuration);

        int size = 30000;
        int block = 3000;
        int chunk = 300;

        int[][] matrixA = MatrixGenerator.createMatrix(size, size);
        int[][] matrixB = MatrixGenerator.createMatrix(size, size);
        long startTime = System.currentTimeMillis();
        executeTasks(matrixA, matrixB, block, chunk);
        long executionTime = System.currentTimeMillis() - startTime;
        System.out.println("Execution time: " + executionTime + "ms");
    }

    public static void executeTasks(int[][] matrixA, int[][] matrixB, int blockSize, int chunkSize) throws InterruptedException, ExecutionException {
        partialResults = hazelcast.getMap("partialResults");

        int totalBlocks = matrixA.length / blockSize;
        CountDownLatch latch = new CountDownLatch(totalBlocks * totalBlocks);

        ExecutorService executor = hazelcast.getExecutorService("taskExecutor");

        for (int x = 0; x < totalBlocks; x++) {
            for (int y = 0; y < totalBlocks; y++) {
                int[][] blockA = extractSubMatrix(matrixA, x * blockSize, y * blockSize, blockSize);
                int[][] blockB = extractSubMatrix(matrixB, x * blockSize, y * blockSize, blockSize);

                Callable<int[][]> task = new MatrixBlockTask(blockA, blockB, chunkSize, "Strassen");
                Future<int[][]> future = executor.submit(task);

                partialResults.put(x + ":" + y, future.get());
                latch.countDown();
            }
        }

        latch.await();
        buildFinalMatrix(matrixA.length, blockSize, 20);
    }

    private static int[][] extractSubMatrix(int[][] matrix, int startRow, int startCol, int dimension) {
        int[][] subMatrix = new int[dimension][dimension];
        for (int i = 0; i < dimension; i++) {
            System.arraycopy(matrix[startRow + i], startCol, subMatrix[i], 0, dimension);
        }
        return subMatrix;
    }

    public static void buildFinalMatrix(int size, int blockSize, int n) {
        int[][] finalMatrix = new int[size][size];

        int totalBlocks = size / blockSize;
        for (int x = 0; x < totalBlocks; x++) {
            for (int y = 0; y < totalBlocks; y++) {
                int[][] block = partialResults.get(x + ":" + y);
                for (int i = 0; i < block.length; i++) {
                    System.arraycopy(block[i], 0, finalMatrix[x * blockSize + i], y * blockSize, block[i].length);
                }
            }
        }

        System.out.println("Matrix size: " + size + " x " + size);
        System.out.print("First " + n + " numbers: ");
        for (int i = 0, count = 0; i < size && count < n; i++) {
            for (int j = 0; j < size && count < n; j++, count++) {
                System.out.print(finalMatrix[i][j] + " ");
            }
        }
        System.out.println();
        System.out.println("Multiplication complete.");
    }
}
