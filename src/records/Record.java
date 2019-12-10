package records;

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

    public double getProbabilityOfA() {
        return probabilityOfA;
    }

    public double getProbabilityOfB() {
        return probabilityOfB;
    }

    public double getProbabilityOfUnion() {
        return probabilityOfUnion;
    }

    @Override
    public int compareTo(Record record) {
        if(record == null)
            return 1;
        return Integer.compareUnsigned(index, record.index);
    }

    @Override
    public String toString(){
        return Integer.toUnsignedString(index) + " " + probabilityOfA + " " + probabilityOfB + " " + probabilityOfUnion;
    }

    private void generateRandomProbs(){
        Random random = new Random();
        index = random.nextInt();
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
