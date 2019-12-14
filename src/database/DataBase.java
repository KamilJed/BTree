package database;

import database.indexes.btree.BTreeIndex;
import database.indexes.btree.BTreeRecord;
import records.Record;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class DataBase {
    private File database;
    private BTreeIndex index;
    private File freeSpaceFile;

    public DataBase(File db) throws IOException{
        database = db;
        index = new BTreeIndex(new File(db.getName() + "index"));
        freeSpaceFile = new File(db.getName() + "free");
        if(!freeSpaceFile.exists())
            throw new FileNotFoundException("Free space file does not exist. Database corrupted");
    }

    public DataBase(String dbPath) throws IOException{
        database = new File(dbPath);
        if(!database.createNewFile())
            throw new IOException();
         index = new BTreeIndex(dbPath + "index");
         freeSpaceFile = new File(dbPath + "free");
         if(!freeSpaceFile.createNewFile())
             throw new IOException();
         RandomAccessFile accessFile = new RandomAccessFile(freeSpaceFile, "rw");
         accessFile.seek(0);
         accessFile.writeInt(0);
         accessFile.close();
    }

    public boolean addToDataBase(Record record){
        try{
            RandomAccessFile accessFile = new RandomAccessFile(database, "rw");
            accessFile.seek(getFreeSpace());
            int address = (int)accessFile.getFilePointer();
            accessFile.write(record.toByteArray());
            accessFile.close();
            return index.insertRecord(record.getIndex(), address);
        }
        catch (IOException e){
            e.printStackTrace();
            return false;
        }
    }

    public Record getRecord(int id){
        BTreeRecord record = index.searchRecord(id);
        if(record == null)
            return null;
        return getRecord(record);
    }

    public Record getRecord(BTreeRecord record){
        try{
            RandomAccessFile accessFile = new RandomAccessFile(database, "r");
            accessFile.seek(record.getRecordAddress());
            byte[] data = new byte[Record.getByteSize()];
            accessFile.read(data);
            accessFile.close();
            return new Record(data);
        }
        catch (IOException e){
            e.printStackTrace();
            return null;
        }
    }

    public boolean deleteRecord(int id){
        BTreeRecord record = index.searchRecord(id);
        if(record == null)
            return false;
        index.deleteRecord(id);
        try{
            RandomAccessFile accessFile = new RandomAccessFile(freeSpaceFile, "rw");
            accessFile.seek(0);
            int freeRecords = accessFile.readInt();
            accessFile.seek(freeRecords*Integer.BYTES + Integer.BYTES);
            accessFile.writeInt(record.getRecordAddress());
            freeRecords++;
            accessFile.seek(0);
            accessFile.writeInt(freeRecords);
            accessFile.close();
            return true;
        }
        catch (IOException e){
            e.printStackTrace();
            return false;
        }
    }

    public boolean updateRecord(int id, int newId, double pA, double pB, double pUnion){
        if(!deleteRecord(id))
            return false;
        return addToDataBase(new Record(newId, pA, pB, pUnion));
    }

    public void printIndex(){
        index.print();
    }

    public void printRawDatabase(){
        try{
            RandomAccessFile accessFile = new RandomAccessFile(database, "r");
            accessFile.seek(0);
            byte[] data = new byte[Record.getByteSize()];
            while(accessFile.read(data) != -1)
                System.out.println(new Record(data));

        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public void printRawIndex(){
        index.printRaw();
    }

    public void printSorted(){
        ArrayList<BTreeRecord> records = index.getSorted();
        for(BTreeRecord record : records)
            System.out.println(getRecord(record));
    }

    public int getPageAccessesNumber(){
        return index.getPageAccessCounter();
    }

    @Override
    public String toString() {
        return database.getName();
    }

    private long getFreeSpace(){
        try{
            int freeAddress;
            RandomAccessFile accessFile = new RandomAccessFile(freeSpaceFile, "rw");
            accessFile.seek(0);
            int freeRecords = accessFile.readInt();
            if(freeRecords > 0){
                accessFile.seek(freeRecords * Integer.BYTES);
                freeAddress = accessFile.readInt();
                accessFile.seek(0);
                freeRecords--;
                accessFile.writeInt(freeRecords);
                accessFile.close();
                return freeAddress;
            }
            RandomAccessFile accessFile1 = new RandomAccessFile(database, "r");
            freeAddress = (int)accessFile1.length();
            accessFile.close();
            return freeAddress;
        }
        catch (IOException e){
            e.printStackTrace();
            return 0;
        }
    }
}
