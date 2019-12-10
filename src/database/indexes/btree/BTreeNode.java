package database.indexes.btree;

import javafx.util.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class BTreeNode {
    public static final int D = 5;
    private int firstChildAddress;
    private int parentPageAddress;
    private int recordsNumber;
    private int selfAddress;
    private ArrayList<BTreeRecord> records;
    private BTreeIndex bTree;

    public BTreeNode(BTreeIndex bTree, int firstChildAddress, int parentPageAddress, int selfAddress, int recordsNumber, byte[] nodeData){
        this.bTree = bTree;
        this.firstChildAddress = firstChildAddress;
        this.parentPageAddress = parentPageAddress;
        this.recordsNumber = recordsNumber;
        this.selfAddress = selfAddress;
        records = new ArrayList<>();
        int i = 0;
        while(i < nodeData.length){
            records.add(new BTreeRecord(nodeData, i));
            i += BTreeRecord.getByteSize();
        }
    }

    public byte[] toByteArray(){
        byte[] nodeData = new byte[3*Integer.BYTES + 2*BTreeNode.D*BTreeRecord.getByteSize()];
        ByteBuffer buffer = ByteBuffer.wrap(nodeData);
        buffer.putInt(firstChildAddress).putInt(parentPageAddress).putInt(recordsNumber);
        for(BTreeRecord record : records){
            buffer.put(record.toByteArray());
        }
        return nodeData;
    }

    public BTreeRecord search(int index){
        Optional<BTreeRecord> record = records.stream().filter(r -> r.getIndex() == index).findFirst();
        if(record.isPresent())
            return record.get();
        else
            return null;
    }

    public int getNextAddress(int index){
        if(records.isEmpty())
            return 0;
        if(index < records.get(0).getIndex())
            return firstChildAddress;
        if(index > records.get(records.size() - 1).getIndex())
            return records.get(records.size() - 1).getChildPageAddress();
        for(int i = 0; i < records.size(); i++){
            if(records.get(i).getIndex() < index && i + 1 < records.size() && records.get(i + 1).getIndex() > index)
                return records.get(i).getChildPageAddress();
        }
        return 0;
    }

    public void insert(BTreeRecord newRecord){
        if(recordsNumber < 2*BTreeNode.D){
            records.add(newRecord);
            Collections.sort(records);
            recordsNumber++;
            bTree.saveNode(this);
        }
        else{
            boolean compansated = compensation();
            if(!compansated){
                try{
                    split(newRecord);
                }
                catch (IOException e){
                    System.out.println("ERROR! Couldn't split page");
                }
            }
        }
    }

    public int getParentPageAddress() {
        return parentPageAddress;
    }

    public int getSelfAddress() {
        return selfAddress;
    }

    public int getFirstChildAddress() {
        return firstChildAddress;
    }

    public List<BTreeRecord> getRecords() {
        return records;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(".");
        for(BTreeRecord record : records){
            builder.append(record.getIndex());
            builder.append(".");
        }
        return builder.toString();
    }

    private boolean compensation(){
        Pair<BTreeNode, BTreeRecord> siblingAndRecord = getFreeSibling();
        if(siblingAndRecord != null){
            ArrayList<BTreeRecord> compensationArray = new ArrayList<>();
            compensationArray.addAll(records);
            compensationArray.add(siblingAndRecord.getValue());
            compensationArray.addAll(siblingAndRecord.getKey().records);
            Collections.sort(compensationArray);
            records.clear();
            siblingAndRecord.getKey().records.clear();
            int i = 0;
            for(; i < compensationArray.size()/2 - 1; i++)
                records.add(compensationArray.get(i));
            try{
                BTreeRecord middleRecord = compensationArray.get(i++);
                int address = siblingAndRecord.getValue().getChildPageAddress();
                BTreeNode parent = bTree.getNode(parentPageAddress);
                parent.records.removeIf(record -> record.getIndex() == siblingAndRecord.getValue().getIndex());
                siblingAndRecord.getValue().setChildPageAddress(middleRecord.getChildPageAddress());
                middleRecord.setChildPageAddress(address);
                parent.records.add(middleRecord);
                Collections.sort(parent.records);
                for(; i < compensationArray.size(); i++)
                    siblingAndRecord.getKey().records.add(compensationArray.get(i));
                parent.recordsNumber = parent.records.size();
                siblingAndRecord.getKey().recordsNumber = siblingAndRecord.getKey().records.size();
                this.recordsNumber = this.records.size();
                bTree.saveNode(this);
                bTree.saveNode(siblingAndRecord.getKey());
                bTree.saveNode(parent);
                return true;
            }
            catch (IOException e){
                e.printStackTrace();
                return false;
            }
        }
        else
            return false;
    }

    private void split(BTreeRecord r) throws IOException{
        int freeAddress = bTree.getFreeAddress();
        if(freeAddress == -1)
            throw new IOException("ERROR! No free address for new node");
        BTreeNode newNode = new BTreeNode(bTree, 0, parentPageAddress, freeAddress, 0, new byte[0]);
        records.add(r);
        Collections.sort(records);
        for(int i = recordsNumber/2 + 1; i < records.size(); i++)
            newNode.records.add(records.get(i));
        newNode.recordsNumber = newNode.records.size();
        records.removeAll(newNode.records);
        BTreeRecord middleRecord = records.get(records.size() - 1);
        records.remove(middleRecord);
        recordsNumber = records.size();
        middleRecord.setChildPageAddress(newNode.selfAddress);
        newNode.updateChilds();
        bTree.saveNode(newNode);
        bTree.saveNode(this);

        if(parentPageAddress == 0){
            freeAddress = bTree.getFreeAddress();
            newNode.parentPageAddress = freeAddress;
            this.parentPageAddress = freeAddress;
            if(freeAddress == -1)
                throw new IOException("ERROR! No free address for new node");
            BTreeNode newRoot = new BTreeNode(bTree, selfAddress, 0, freeAddress, 0, new byte[0]);
            newRoot.insert(middleRecord);
            bTree.saveNode(this);
            bTree.saveNode(newNode);
        }
        else{
            BTreeNode node = bTree.getNode(parentPageAddress);
            node.insert(middleRecord);
        }
    }

    private void updateChilds(){
        try{
            BTreeNode node = bTree.getNode(firstChildAddress);
            node.parentPageAddress = selfAddress;
            bTree.saveNode(node);
            for(BTreeRecord record : records){
                node = bTree.getNode(record.getChildPageAddress());
                node.parentPageAddress = selfAddress;
                bTree.saveNode(node);
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private Pair<BTreeNode, BTreeRecord> getFreeSibling(){
        if(parentPageAddress == 0)
            return null;
        try{
            BTreeNode parent = bTree.getNode(parentPageAddress);
            if(selfAddress == parent.firstChildAddress){
                if(parent.records.get(0).getChildPageAddress() != 0){
                    BTreeNode sibling = bTree.getNode(parent.records.get(0).getChildPageAddress());
                    if(sibling.recordsNumber < 2*BTreeNode.D)
                        return new Pair<>(sibling, parent.records.get(0));
                }
            }
            if(selfAddress == parent.records.get(parent.records.size() - 1).getChildPageAddress()){
                BTreeNode sibling;
                if(parent.records.size() == 1)
                    sibling = bTree.getNode(parent.firstChildAddress);
                else
                    sibling = bTree.getNode(parent.records.get(parent.records.size() - 2).getChildPageAddress());
                if(sibling.recordsNumber < 2*BTreeNode.D)
                    return new Pair<>(sibling, parent.records.get(parent.records.size() - 1));
            }
            int index = records.get(0).getIndex();
            for(int i = 0; i < parent.records.size(); i++){
                if(parent.records.get(i).getIndex() < index && i + 1 < parent.records.size() && parent.records.get(i + 1).getIndex() > index){
                    BTreeNode sibling;
                    if(i == 0){
                        sibling = bTree.getNode(parent.firstChildAddress);
                    }
                    else{
                        sibling = bTree.getNode(parent.records.get(i - 1).getChildPageAddress());
                    }
                    if(sibling.recordsNumber < 2*BTreeNode.D)
                        return new Pair<>(sibling, parent.records.get(i - 1));

                    sibling = bTree.getNode(parent.records.get(i + 1).getChildPageAddress());
                    if(sibling.recordsNumber < 2*BTreeNode.D)
                        return new Pair<>(sibling, parent.records.get(i + 1));
                }
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }
}
