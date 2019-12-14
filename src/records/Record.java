package records;

import java.nio.ByteBuffer;
import java.util.Random;

public class Record implements Comparable<Record>{

    private int index;
    private double probabilityOfA;
    private double probabilityOfB;
    private double probabilityOfUnion;

    public Record(){
        generateRandomProbs();
    }

    public Record(int index, double pA, double pB, double pAB){
        this.index = index;
        probabilityOfA = pA;
        probabilityOfB = pB;
        probabilityOfUnion = pAB;
    }

    public Record(byte[] data){
        int iter = 0;
        index = ByteBuffer.wrap(data).getInt(iter);
        iter += Integer.BYTES;
        probabilityOfA = ByteBuffer.wrap(data).getDouble(iter);
        iter += Double.BYTES;
        probabilityOfB = ByteBuffer.wrap(data).getDouble(iter);
        iter += Double.BYTES;
        probabilityOfUnion = ByteBuffer.wrap(data).getDouble(iter);
    }

    public double getProbabilityOfA() {
        return probabilityOfA;
    }

    public double getProbabilityOfB() {
        return probabilityOfB;
    }

    public double getProbabilityOfUnion() {
        return probabilityOfUnion;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public int compareTo(Record record) {
        if(record == null)
            return 1;
        return Integer.compareUnsigned(index, record.index);
    }

    @Override
    public String toString(){
        return index + " " + probabilityOfA + " " + probabilityOfB + " " + probabilityOfUnion;
    }

    public byte[] toByteArray(){
        byte[] data = new byte[getByteSize()];
        ByteBuffer.wrap(data).putInt(index).putDouble(probabilityOfA).putDouble(probabilityOfB).putDouble(probabilityOfUnion);
        return data;
    }

    public static int getByteSize(){
        return Integer.BYTES + 3*Double.BYTES;
    }

    private void generateRandomProbs(){
        Random random = new Random();
        index = random.nextInt() & Integer.MAX_VALUE;
        do{
            probabilityOfA = random.nextDouble();
            probabilityOfB = random.nextDouble();
            probabilityOfUnion = random.nextDouble();
            probabilityOfUnion = random.nextDouble();
        }
        while(intersectionProbability() > 1 || intersectionProbability() < 0);
    }

    private double intersectionProbability(){
        return probabilityOfA + probabilityOfB - probabilityOfUnion;
    }
}
