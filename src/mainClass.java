import database.DataBase;
import database.indexes.btree.BTreeIndex;

import java.io.IOException;

public class mainClass {

    public static void main(String[] args) {
        try{
            BTreeIndex index = new BTreeIndex("index");
            index.insertRecord(12, 1221);
            index.insertRecord(1, 1221);
            index.insertRecord(24, 1221);
            index.insertRecord(2, 1221);
            index.insertRecord(4, 1221);
            index.insertRecord(5, 1221);
            index.insertRecord(6, 1221);
            index.insertRecord(7, 1221);
            index.insertRecord(8, 1221);
            index.insertRecord(9, 1221);
            index.insertRecord(11, 1221);
            index.insertRecord(13, 1221);
            index.insertRecord(14, 1221);
            index.insertRecord(15, 1221);
            index.insertRecord(16, 1221);
            index.insertRecord(17, 1221);
            index.print();
            index.insertRecord(18, 1221);
            System.out.println("------------------------------");
            index.print();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
}
