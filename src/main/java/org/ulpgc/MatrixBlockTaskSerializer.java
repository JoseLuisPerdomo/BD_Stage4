package org.ulpgc;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import java.io.IOException;

public class MatrixBlockTaskSerializer implements StreamSerializer<MatrixBlockTask> {

    @Override
    public void write(ObjectDataOutput out, MatrixBlockTask task) throws IOException {
        out.writeInt(task.blockA.length);
        out.writeInt(task.blockA[0].length);
        for (int[] row : task.blockA) {
            for (int value : row) {
                out.writeInt(value);
            }
        }

        out.writeInt(task.blockB.length);
        out.writeInt(task.blockB[0].length);
        for (int[] row : task.blockB) {
            for (int value : row) {
                out.writeInt(value);
            }
        }

        out.writeInt(task.chunkSize);
    }

    @Override
    public MatrixBlockTask read(ObjectDataInput in) throws IOException {
        int rowsA = in.readInt();
        int colsA = in.readInt();
        int[][] blockA = new int[rowsA][colsA];
        for (int i = 0; i < rowsA; i++) {
            for (int j = 0; j < colsA; j++) {
                blockA[i][j] = in.readInt();
            }
        }

        int rowsB = in.readInt();
        int colsB = in.readInt();
        int[][] blockB = new int[rowsB][colsB];
        for (int i = 0; i < rowsB; i++) {
            for (int j = 0; j < colsB; j++) {
                blockB[i][j] = in.readInt();
            }
        }

        int chunkSize = in.readInt();
        return new MatrixBlockTask(blockA, blockB, chunkSize, "Strassen");
    }

    @Override
    public int getTypeId() {
        return 2;
    }

    @Override
    public void destroy() {
    }
}