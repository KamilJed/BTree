package database.indexes.btree;

import javafx.util.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class BTreeNode {
    public static final int D = 2;
    private static final int SIBLING_INSERT = 0;
    private static final int SIBLING_DELETE = 1;
    private static final int SIBLING_MERGE = 2;
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
            boolean compansated = compensation(newRecord, BTreeNode.SIBLING_INSERT);
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

    public void delete(int index) throws NullPointerException{
        if(firstChildAddress == 0){
            records.remove(search(index));
            recordsNumber--;
            afterDel();
        }
        else{
            BTreeRecord toDelRecord = search(index);
            BTreeNode minNode = getSuccecessor(toDelRecord.getChildPageAddress());
            if(minNode == null)
                throw new NullPointerException("ERROR! Couldn't get min node");
            BTreeRecord successor = minNode.records.get(0);
            records.remove(toDelRecord);
            successor.setChildPageAddress(toDelRecord.getChildPageAddress());
            records.add(successor);
            Collections.sort(records);
            minNode.records.remove(successor);
            minNode.recordsNumber--;
            bTree.saveNode(this);
            minNode.afterDel();
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
            builder.append(record);
            builder.append(".");
        }
        builder.append(" (" + selfAddress + ") " + "(" + parentPageAddress + ")");
        return builder.toString();
    }

    private BTreeNode getSuccecessor(int address){
        try{
            BTreeNode minNode = bTree.getNode(address);
            while(minNode.firstChildAddress != 0)
                minNode = bTree.getNode(minNode.firstChildAddress);
            return minNode;
        }
        catch (IOException e){
            e.printStackTrace();
            return null;
        }
    }

    private void afterDel(){
        if(recordsNumber < BTreeNode.D && parentPageAddress != 0){
            boolean compansated = compensation(null, BTreeNode.SIBLING_DELETE);
            if(!compansated){
                merge();
            }
        }
        else
            bTree.saveNode(this);
    }

    private boolean compensation(BTreeRecord r, int mode){
        Pair<BTreeNode, BTreeRecord> siblingAndRecord = getFreeSibling(mode);

        if(siblingAndRecord != null){
            ArrayList<BTreeRecord> compensationArray = new ArrayList<>();
            compensationArray.addAll(records);
            compensationArray.add(siblingAndRecord.getValue());
            if(r != null)
                compensationArray.add(r);
            compensationArray.addAll(siblingAndRecord.getKey().records);
            Collections.sort(compensationArray);
            ArrayList<BTreeRecord> smaller, bigger;
            if(records.get(0).getIndex() < siblingAndRecord.getValue().getIndex()){
                smaller = records;
                bigger = siblingAndRecord.getKey().records;
            }
            else{
                smaller = siblingAndRecord.getKey().records;
                bigger = records;
            }
            records.clear();
            siblingAndRecord.getKey().records.clear();
            int i = 0;
            for(; i < compensationArray.size()/2; i++)
                smaller.add(compensationArray.get(i));
            try{
                BTreeRecord middleRecord = compensationArray.get(i++);
                int address = siblingAndRecord.getValue().getChildPageAddress();
                BTreeNode parent = bTree.getNode(parentPageAddress);
                if(this.records == smaller){
                    siblingAndRecord.getValue().setChildPageAddress(siblingAndRecord.getKey().firstChildAddress);
                    siblingAndRecord.getKey().firstChildAddress = middleRecord.getChildPageAddress();
                    middleRecord.setChildPageAddress(address);
                }
                else{
                    siblingAndRecord.getValue().setChildPageAddress(this.firstChildAddress);
                    this.firstChildAddress = middleRecord.getChildPageAddress();
                    middleRecord.setChildPageAddress(address);
                }
                parent.records.removeIf(record -> record.getIndex() == siblingAndRecord.getValue().getIndex());
                parent.records.add(middleRecord);
                Collections.sort(parent.records);
                for(; i < compensationArray.size(); i++)
                    bigger.add(compensationArray.get(i));
                parent.recordsNumber = parent.records.size();
                siblingAndRecord.getKey().recordsNumber = siblingAndRecord.getKey().records.size();
                this.recordsNumber = this.records.size();
                updateChilds();
                siblingAndRecord.getKey().updateChilds();
                parent.updateChilds();
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

    private void merge(){
        Pair<BTreeNode, BTreeRecord> siblingAndRecord = getFreeSibling(BTreeNode.SIBLING_MERGE);
        BTreeNode smaller, bigger;
        if(records.get(0).getIndex() < siblingAndRecord.getValue().getIndex()){
            smaller = this;
            bigger = siblingAndRecord.getKey();
        }
        else{
            smaller = siblingAndRecord.getKey();
            bigger = this;
        }
        smaller.records.addAll(bigger.records);
        siblingAndRecord.getValue().setChildPageAddress(bigger.firstChildAddress);
        smaller.records.add(siblingAndRecord.getValue());
        Collections.sort(smaller.records);
        try{
            BTreeNode parent = bTree.getNode(parentPageAddress);
            parent.records.removeIf(record -> record.getIndex() == siblingAndRecord.getValue().getIndex());
            parent.recordsNumber--;
            bigger.deletePage();
            if(parent.parentPageAddress == 0 && parent.recordsNumber == 0){
                smaller.parentPageAddress = 0;
                smaller.recordsNumber = smaller.records.size();
                smaller.updateChilds();
                bTree.saveNode(smaller);
                parent.deletePage();
            }
            else{
                smaller.recordsNumber = smaller.records.size();
                smaller.updateChilds();
                bTree.saveNode(smaller);
                parent.recordsNumber = parent.records.size();
                parent.updateChilds();
                parent.afterDel();
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
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
        newNode.firstChildAddress = middleRecord.getChildPageAddress();
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
            BTreeNode node;
            if(firstChildAddress != 0) {
                node = bTree.getNode(firstChildAddress);
                node.parentPageAddress = selfAddress;
                bTree.saveNode(node);
            }
            for(BTreeRecord record : records){
                if(record.getChildPageAddress() != 0){
                    node = bTree.getNode(record.getChildPageAddress());
                    node.parentPageAddress = selfAddress;
                    bTree.saveNode(node);
                }
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private void deletePage(){
        parentPageAddress = -1;
        firstChildAddress = -1;
        recordsNumber = -1;
        records.clear();
        for(int i = 0; i < 2*BTreeNode.D; i++)
            records.add(new BTreeRecord(-1, -1, -1));
        bTree.saveNode(this);
        bTree.addFreeAddress(selfAddress);
    }

    private Pair<BTreeNode, BTreeRecord> getFreeSibling(int mode){
        if(parentPageAddress == 0)
            return null;
        try{
            BTreeNode parent = bTree.getNode(parentPageAddress);
            if(selfAddress == parent.firstChildAddress){
                if(parent.records.get(0).getChildPageAddress() != 0){
                    BTreeNode sibling = bTree.getNode(parent.records.get(0).getChildPageAddress());
                    if(mode == BTreeNode.SIBLING_INSERT && sibling.recordsNumber < 2*BTreeNode.D)
                        return new Pair<>(sibling, parent.records.get(0));
                    else if(mode == BTreeNode.SIBLING_DELETE && sibling.recordsNumber > BTreeNode.D)
                        return new Pair<>(sibling, parent.records.get(0));
                    else if(mode == BTreeNode.SIBLING_MERGE)
                        return new Pair<>(sibling, parent.records.get(0));
                }
            }
            if(selfAddress == parent.records.get(parent.records.size() - 1).getChildPageAddress()){
                BTreeNode sibling;
                if(parent.records.size() == 1)
                    sibling = bTree.getNode(parent.firstChildAddress);
                else
                    sibling = bTree.getNode(parent.records.get(parent.records.size() - 2).getChildPageAddress());
                if(mode == BTreeNode.SIBLING_INSERT && sibling.recordsNumber < 2*BTreeNode.D)
                    return new Pair<>(sibling, parent.records.get(parent.records.size() - 1));
                else if(mode == BTreeNode.SIBLING_DELETE && sibling.recordsNumber > BTreeNode.D)
                    return new Pair<>(sibling, parent.records.get(parent.records.size() - 1));
                else if(mode == BTreeNode.SIBLING_MERGE)
                    return new Pair<>(sibling, parent.records.get(parent.records.size() - 1));
            }
            int index = records.get(0).getIndex();
            for(int i = 0; i < parent.records.size(); i++){
                if(parent.records.get(i).getIndex() < index && i + 1 < parent.records.size() && parent.records.get(i + 1).getIndex() > index){
                    BTreeNode sibling;
                    if(i == 0){
                        sibling = bTree.getNode(parent.firstChildAddress);
                        if(mode == BTreeNode.SIBLING_INSERT && sibling.recordsNumber < 2*BTreeNode.D)
                            return new Pair<>(sibling, parent.records.get(0));
                        else if(mode == BTreeNode.SIBLING_DELETE && sibling.recordsNumber > BTreeNode.D)
                            return new Pair<>(sibling, parent.records.get(0));
                        else if(mode == BTreeNode.SIBLING_MERGE)
                            return new Pair<>(sibling, parent.records.get(0));
                    }
                    else{
                        sibling = bTree.getNode(parent.records.get(i - 1).getChildPageAddress());
                        if(mode == BTreeNode.SIBLING_INSERT && sibling.recordsNumber < 2*BTreeNode.D)
                            return new Pair<>(sibling, parent.records.get(i));
                        else if(mode == BTreeNode.SIBLING_DELETE && sibling.recordsNumber > BTreeNode.D)
                            return new Pair<>(sibling, parent.records.get(i));
                        else if(mode == BTreeNode.SIBLING_MERGE)
                            return new Pair<>(sibling, parent.records.get(i));
                    }

                    sibling = bTree.getNode(parent.records.get(i + 1).getChildPageAddress());
                    if(mode == BTreeNode.SIBLING_INSERT && sibling.recordsNumber < 2*BTreeNode.D)
                        return new Pair<>(sibling, parent.records.get(i + 1));
                    else if(mode == BTreeNode.SIBLING_DELETE && sibling.recordsNumber > BTreeNode.D)
                        return new Pair<>(sibling, parent.records.get(i + 1));
                    else if(mode == BTreeNode.SIBLING_MERGE)
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
