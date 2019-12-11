package database.indexes.btree;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class BTreeIndex {
    private File indexFile;
    private BTreeNode root = null;

    public BTreeIndex(String indexPath) throws IOException{
        indexFile = new File(indexPath);
        if(!indexFile.createNewFile())
            throw new IOException("ERROR! Couldn't create index file");
        RandomAccessFile accessFile = new RandomAccessFile(indexFile, "rw");
        accessFile.seek(0);
        accessFile.writeInt(4);
        accessFile.write(new BTreeNode(this,0, 0, 4,0, new byte[0]).toByteArray());
        root = getRoot();
    }

    public BTreeIndex(File indexFile) throws IOException, FileNotFoundException{
        this.indexFile = indexFile;
        try{
            root = getRoot();
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
            RandomAccessFile accessFile = new RandomAccessFile(indexFile, "r");
            long len = accessFile.length();
            return (int)len;
        }
        catch (IOException e){
            System.out.println("ERROR! Couldn't get free space");
        }
        return -1;
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

    public void printSorted(){
        try{
            RandomAccessFile accessFile = new RandomAccessFile(indexFile, "r");
            accessFile.seek(0);
            int rootAddress = accessFile.readInt();
            accessFile.close();
            printSortedTree(rootAddress);
            System.out.println("--------------------------");
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private void printSortedTree(int address){
        try{
            BTreeNode node = getNode(address);
            if(node.getFirstChildAddress() != 0)
                printSortedTree(node.getFirstChildAddress());
            for(BTreeRecord record : node.getRecords()){
                System.out.println(record);
                if(record.getChildPageAddress() != 0)
                    printSortedTree(record.getChildPageAddress());
            }
        }
        catch (IOException e){
            e.printStackTrace();
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
        return getNode(rootAddress);
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
