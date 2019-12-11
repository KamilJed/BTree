import database.DataBase;
import records.Record;

import java.io.IOException;
import java.util.Scanner;

public class mainClass {

    public static void main(String[] args) {
        try{
            DataBase db = new DataBase("database");
//            for(int i = 0; i < 25; i++){
//                db.addToDataBase(new Record());
//            }
            while(true){
                Scanner scanner = new Scanner(System.in);
                String command = scanner.nextLine();
                int id, newId;
                double pA, pB, pUnion;
                switch (command.charAt(0)){
                    case 'a':
                        id = Integer.parseInt(command.split(" ")[1]);
                        pA = Double.parseDouble(command.split(" ")[2]);
                        pB = Double.parseDouble(command.split(" ")[3]);
                        pUnion = Double.parseDouble(command.split(" ")[4]);
                        if(db.addToDataBase(new Record(id, pA, pB, pUnion)))
                            System.out.println("Record added successfully");
                        else
                            System.out.println("Record already in database");
                        break;
                    case 'g':
                        id = Integer.parseInt(command.split(" ")[1]);
                        Record r = db.getRecord(id);
                        if(r != null)
                            System.out.println(r);
                        else
                            System.out.println("No such record in database");
                        break;
                    case 'd':
                        id = Integer.parseInt(command.split(" ")[1]);
                        if(db.deleteRecord(id))
                            System.out.println("Record deleted");
                        else
                            System.out.println("No such record in database");
                        break;
                    case 'u':
                        id = Integer.parseInt(command.split(" ")[1]);
                        newId = Integer.parseInt(command.split(" ")[2]);
                        break;
                    case 'p':
                        db.printIndex();
                        break;
                    case 's':
                        db.printSortedIndex();
                        break;
                }
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
}
