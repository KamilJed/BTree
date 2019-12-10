package database.indexes.btree;

import java.nio.ByteBuffer;

public class BTreeRecord implements Comparable<BTreeRecord> {
    private int index;
    private int recordAddress;
    private int childPageAddress;

    static public int getByteSize(){
        return 3*Integer.BYTES;
    }

    public BTreeRecord(int index, int recordAddress, int childPageAddress){
        this.index = index;
        this.recordAddress = recordAddress;
        this.childPageAddress = childPageAddress;
    }

    public BTreeRecord(byte[] reocordsData, int iter){
        index = ByteBuffer.wrap(reocordsData).getInt(iter);
        iter += Integer.BYTES;
        recordAddress = ByteBuffer.wrap(reocordsData).getInt(iter);
        iter += Integer.BYTES;
        childPageAddress = ByteBuffer.wrap(reocordsData).getInt(iter);
    }

    public byte[] toByteArray(){
        byte[] data = new byte[BTreeRecord.getByteSize()];
        ByteBuffer.wrap(data).putInt(index).putInt(recordAddress).putInt(childPageAddress);
        return data;
    }

    @Override
    public int compareTo(BTreeRecord record) {
        return Integer.compareUnsigned(index, record.index);
    }

    public int getChildPageAddress() {
        return childPageAddress;
    }

    public int getRecordAddress() {
        return recordAddress;
    }

    public int getIndex() {
        return index;
    }

    public void setChildPageAddress(int childPageAddress) {
        this.childPageAddress = childPageAddress;
    }
}

