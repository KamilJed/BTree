import database.DataBase;
import database.indexes.btree.BTreeIndex;
import database.indexes.btree.BTreeRecord;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class mainClass {

    public static void main(String[] args) {
        try{
            BTreeIndex index = new BTreeIndex("index");
            for(int i = 0; i < 25; i++){
                Random r = new Random();
                index.insertRecord(r.nextInt(1000), 10);
            }
            while(true){
                Scanner scanner = new Scanner(System.in);
                String command = scanner.nextLine();
                int id, newId;
                switch (command.charAt(0)){
                    case 'a':
                        id = Integer.parseInt(command.split(" ")[1]);
                        index.insertRecord(id, 9);
                        break;
                    case 'd':
                        id = Integer.parseInt(command.split(" ")[1]);
                        index.deleteRecord(id);
                        break;
                    case 'u':
                        id = Integer.parseInt(command.split(" ")[1]);
                        newId = Integer.parseInt(command.split(" ")[2]);
                        index.updateRecord(id, newId);
                        break;
                    case 'p':
                        index.print();
                        break;
                    case 's':
                        index.printSorted();
                        break;
                }
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
}
