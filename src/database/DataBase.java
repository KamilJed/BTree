package database;

import database.indexes.btree.BTreeIndex;

import java.io.File;
import java.io.IOException;

public class DataBase {
    private File database;
    private BTreeIndex index;

    public DataBase(File db, BTreeIndex index){
        database = db;
        this.index = index;
    }

    public DataBase(String dbPath) throws IOException{
        database = new File(dbPath);
        if(!database.createNewFile())
            throw new IOException();
         index = new BTreeIndex(dbPath + "index");
    }
}
