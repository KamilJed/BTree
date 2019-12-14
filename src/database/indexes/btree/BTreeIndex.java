package database.indexes.btree;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class BTreeIndex {
    private File indexFile;
    private BTreeNode root = null;
    private File freeSpaceFile;
    private int pageAccessCounter;

    public BTreeIndex(String indexPath) throws IOException{
        indexFile = new File(indexPath);
        if(!indexFile.createNewFile())
            throw new IOException("ERROR! Couldn't create index file");
        RandomAccessFile accessFile = new RandomAccessFile(indexFile, "rw");
        accessFile.seek(0);
        accessFile.writeInt(4);
        accessFile.write(new BTreeNode(this,0, 0, 4,0, new byte[0]).toByteArray());
        accessFile.close();
        root = getRoot();
        pageAccessCounter = 0;
        freeSpaceFile = new File(indexPath + "free");
        if(!freeSpaceFile.createNewFile())
            throw new IOException("ERROR! Couldn't create free space file for index");
        accessFile = new RandomAccessFile(freeSpaceFile, "rw");
        accessFile.seek(0);
        accessFile.writeInt(0);
        accessFile.close();
    }

    public BTreeIndex(File indexFile) throws IOException, FileNotFoundException{
        this.indexFile = indexFile;
        freeSpaceFile = new File(indexFile.getName() + "free");
        if(!freeSpaceFile.exists())
            throw new FileNotFoundException("Free space file does not exist. Index corrupted");
        try{
            root = getRoot();
            pageAccessCounter = 0;
        }
        catch (FileNotFoundException e){
            System.out.println("ERROR! Index does not exist");
            throw new FileNotFoundException("ERROR! Index does not exist");
        }
        catch (IOException e){
            System.out.println("ERROR! Couldn't read from index file");
            throw new IOException("ERROR! Couldn't read from index file");
        }
    }

    public BTreeRecord searchRecord(int index){
        return searchRecordNode(index).search(index);
    }

    public boolean insertRecord(int index, int address){
        BTreeNode node = searchRecordNode(index);
        if(node.search(index) != null)
            return false;
        BTreeRecord newRecord = new BTreeRecord(index, address, 0);
        node.insert(newRecord);
        return true;
    }

    public boolean deleteRecord(int index){
        BTreeNode node = searchRecordNode(index);
        if(node.search(index) == null)
            return false;
        node.delete(index);
        return true;
    }

    public boolean updateRecord(int index, int newIndex){
        BTreeRecord record = searchRecord(index);
        if(record == null)
            return false;
        deleteRecord(index);
        insertRecord(newIndex, record.getRecordAddress());
        return true;
    }

    public void saveNode(BTreeNode node){
        try{
            RandomAccessFile accessFile = new RandomAccessFile(indexFile, "rw");
            accessFile.seek(node.getSelfAddress());
            accessFile.write(node.toByteArray());
            accessFile.close();
            if(node.getParentPageAddress() == 0){
                updateRootAddress(node.getSelfAddress());
            }
            pageAccessCounter++;
        }
        catch (FileNotFoundException e){
            System.out.println("ERROR! Couldn't get index file");
        }
        catch (IOException e){
            System.out.println("ERROR! Couldn't read from index file");
        }
    }

    public int getFreeAddress(){
        try{
            int freeAddress;
            RandomAccessFile accessFile = new RandomAccessFile(freeSpaceFile, "rw");
            accessFile.seek(0);
            int freeRecords = accessFile.readInt();
            if(freeRecords > 0) {
                accessFile.seek(freeRecords * Integer.BYTES);
                freeAddress = accessFile.readInt();
                accessFile.seek(0);
                freeRecords--;
                accessFile.writeInt(freeRecords);
                accessFile.close();
                return freeAddress;
            }

            accessFile = new RandomAccessFile(indexFile, "r");
            long len = accessFile.length();
            accessFile.close();
            return (int)len;
        }
        catch (IOException e){
            System.out.println("ERROR! Couldn't get free space");
        }
        return -1;
    }

    public void addFreeAddress(int address){
        try{
            RandomAccessFile accessFile = new RandomAccessFile(freeSpaceFile, "rw");
            accessFile.seek(0);
            int freeRecords = accessFile.readInt();
            accessFile.seek(freeRecords*Integer.BYTES + Integer.BYTES);
            accessFile.writeInt(address);
            freeRecords++;
            accessFile.seek(0);
            accessFile.writeInt(freeRecords);
            accessFile.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public BTreeNode getNode(int address) throws FileNotFoundException, IOException{
        RandomAccessFile accessFile = new RandomAccessFile(indexFile, "r");
        accessFile.seek(address);
        int firstChildAddress = accessFile.readInt();
        int parentPageAddress = accessFile.readInt();
        int recordsNumber = accessFile.readInt();
        byte[] records = new byte[recordsNumber*BTreeRecord.getByteSize()];
        accessFile.read(records);
        accessFile.close();
        pageAccessCounter++;
        return new BTreeNode(this, firstChildAddress, parentPageAddress, address, recordsNumber, records);
    }

    public void print(){
        try{
            RandomAccessFile accessFile = new RandomAccessFile(indexFile, "r");
            accessFile.seek(0);
            int rootAddress = accessFile.readInt();
            accessFile.close();
            printTree(0, rootAddress);
            System.out.println("--------------------------");
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public void printRaw(){
        try{
            RandomAccessFile accessFile = new RandomAccessFile(indexFile, "r");
            accessFile.seek(Integer.BYTES);
            while(accessFile.getFilePointer() != accessFile.length()){
                int firstChildAddress = accessFile.readInt();
                int parentPageAddress = accessFile.readInt();
                int recordsNumber = accessFile.readInt();
                byte[] records = new byte[2*BTreeNode.D*BTreeRecord.getByteSize()];
                accessFile.read(records);
                System.out.println(new BTreeNode(this, firstChildAddress, parentPageAddress, (int)accessFile.getFilePointer(), recordsNumber, records));
            }
            accessFile.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public ArrayList<BTreeRecord> getSorted(){
        try{
            RandomAccessFile accessFile = new RandomAccessFile(indexFile, "r");
            accessFile.seek(0);
            int rootAddress = accessFile.readInt();
            accessFile.close();
            return getSortedTree(rootAddress);
        }
        catch (IOException e){
            e.printStackTrace();
            return null;
        }
    }

    public double getMemoryUsasge(){
        try{
            RandomAccessFile accessFile = new RandomAccessFile(indexFile, "r");
            accessFile.seek(Integer.BYTES);
            long r = 0, p = 0;
            while(accessFile.getFilePointer() != accessFile.length()){
                int firstChildAddress = accessFile.readInt();
                int parentPageAddress = accessFile.readInt();
                int recordsNumber = accessFile.readInt();
                r += recordsNumber;
                p++;
                byte[] records = new byte[2*BTreeNode.D*BTreeRecord.getByteSize()];
                accessFile.read(records);
            }
            accessFile.close();
            return (double)r/(double)(p*2*BTreeNode.D);
        }
        catch (IOException e){
            e.printStackTrace();
            return 0;
        }
    }

    public int getPageAccessCounter() {
        int counter = pageAccessCounter;
        pageAccessCounter = 0;
        return counter;
    }

    private ArrayList<BTreeRecord> getSortedTree(int address){
        try{
            ArrayList<BTreeRecord> recordList = new ArrayList<>();
            BTreeNode node = getNode(address);
            if(node.getFirstChildAddress() != 0)
                recordList.addAll(getSortedTree(node.getFirstChildAddress()));
            for(BTreeRecord record : node.getRecords()){
                recordList.add(record);
                if(record.getChildPageAddress() != 0)
                    recordList.addAll(getSortedTree(record.getChildPageAddress()));
            }
            return recordList;
        }
        catch (IOException e){
            e.printStackTrace();
            return null;
        }
    }

    private void printTree(int depth, int address){
        try{
            BTreeNode node = getNode(address);
            StringBuilder builder = new StringBuilder();
            for(int i = 0; i < depth; i++)
                builder.append("\t");
            builder.append(node.toString());
            System.out.println(builder);
            if(node.getFirstChildAddress() != 0){
                printTree(depth + 1, node.getFirstChildAddress());
                for(BTreeRecord record : node.getRecords()){
                    printTree(depth + 1, record.getChildPageAddress());
                }
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private BTreeNode searchRecordNode(int index){
        try{
            root = getRoot();
        }
        catch (IOException e){
            System.out.println("ERROR! Couldn't get root");
            root = null;
        }
        if(root == null)
            return null;
        BTreeRecord record = root.search(index);
        if(record == null && root.getNextAddress(index) != 0)
            return deepSearchAddress(index, root.getNextAddress(index));
        else
            return root;
    }

    private BTreeNode getRoot() throws FileNotFoundException, IOException {
        RandomAccessFile accessFile = new RandomAccessFile(indexFile, "r");
        accessFile.seek(0);
        int rootAddress = accessFile.readInt();
        accessFile.close();
        BTreeNode root = getNode(rootAddress);
        return root;
    }

    private BTreeNode deepSearchAddress(int index, int address){
        BTreeNode recordNode = null;
        try{
            BTreeNode node = getNode(address);
            BTreeRecord record = node.search(index);
            int nextAddress;
            if(record == null && (nextAddress = node.getNextAddress(index)) != 0)
                recordNode = deepSearchAddress(index, nextAddress);
            else
                recordNode = node;
        }
        catch (FileNotFoundException e){
            System.out.println("ERROR! Couldn't get index file");
        }
        catch (IOException e){
            System.out.println("ERROR! Couldn't read from index file");
        }
        return recordNode;
    }

    private void updateRootAddress(int address){
        try{
            RandomAccessFile accessFile = new RandomAccessFile(indexFile, "rw");
            accessFile.seek(0);
            accessFile.writeInt(address);
        }
        catch (FileNotFoundException e){
            System.out.println("ERROR! Couldn't get index file");
        }
        catch (IOException e){
            System.out.println("ERROR! Couldn't read from index file");
        }
    }
}
