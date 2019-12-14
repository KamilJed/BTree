import database.DataBase;
import javafx.util.Pair;
import records.Record;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class mainClass {
    private static DataBase db;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        while(true){
            if(db == null)
                System.out.println("No database opened");
            else
                System.out.println("Currently opened: " + db);
            String command = scanner.nextLine();
            if(command.isEmpty())
                command = " ";
            if(!executeCommand(command))
                break;
            }
        }

    public static String getPageAccessesNumber(DataBase db){
        return "Operation took " + db.getPageAccessesNumber() + " page accesses";
    }

    public static boolean executeCommand(String command){
        int id, newId;
        double pA, pB, pUnion;
        String dbName;
        switch (command.charAt(0)){
            case 'a':
                if(db == null){
                    System.out.println("Choose or create a database first");
                    break;
                }
                if(command.split(" ").length != 5){
                    System.out.println("Unrecognised parameters");
                    break;
                }
                id = Integer.parseInt(command.split(" ")[1]);
                pA = Double.parseDouble(command.split(" ")[2]);
                pB = Double.parseDouble(command.split(" ")[3]);
                pUnion = Double.parseDouble(command.split(" ")[4]);
                if(db.addToDataBase(new Record(id, pA, pB, pUnion)))
                    System.out.println("Record added successfully");
                else
                    System.out.println("Record already in database");
                System.out.println(getPageAccessesNumber(db));
                break;
            case 'g':
                if(db == null){
                    System.out.println("Choose or create a database first");
                    break;
                }
                if(command.split(" ").length != 2){
                    System.out.println("Unrecognised parameters");
                    break;
                }
                id = Integer.parseInt(command.split(" ")[1]);
                Record r = db.getRecord(id);
                if(r != null)
                    System.out.println(r);
                else
                    System.out.println("No such record in database");
                System.out.println(getPageAccessesNumber(db));
                break;
            case 'd':
                if(db == null){
                    System.out.println("Choose or create a database first");
                    break;
                }
                if(command.split(" ").length != 2){
                    System.out.println("Unrecognised parameters");
                    break;
                }
                id = Integer.parseInt(command.split(" ")[1]);
                if(db.deleteRecord(id))
                    System.out.println("Record deleted");
                else
                    System.out.println("No such record in database");
                System.out.println(getPageAccessesNumber(db));
                break;
            case 'u':
                if(db == null){
                    System.out.println("Choose or create a database first");
                    break;
                }
                if(command.split(" ").length != 6){
                    System.out.println("Unrecognised parameters");
                    break;
                }
                id = Integer.parseInt(command.split(" ")[1]);
                newId = Integer.parseInt(command.split(" ")[2]);
                pA = Double.parseDouble(command.split(" ")[3]);
                pB = Double.parseDouble(command.split(" ")[4]);
                pUnion = Double.parseDouble(command.split(" ")[5]);
                if(db.updateRecord(id, newId, pA, pB, pUnion))
                    System.out.println("Record updated successfully");
                else
                    System.out.println("No such record in database");
                System.out.println(getPageAccessesNumber(db));
                break;
            case 'p':
                if(db == null){
                    System.out.println("Choose or create a database first");
                    break;
                }
                db.printIndex();
                System.out.println(getPageAccessesNumber(db));
                break;
            case 's':
                if(db == null){
                    System.out.println("Choose or create a database first");
                    break;
                }
                db.printSorted();
                System.out.println(getPageAccessesNumber(db));
                break;
            case 'r':
                if(db == null){
                    System.out.println("Choose or create a database first");
                    break;
                }
                db.printRawDatabase();
                break;
            case 't':
                if(db == null){
                    System.out.println("Choose or create a database first");
                    break;
                }
                db.printRawIndex();
                break;
            case 'q':
                return false;
            case 'i':
                if(command.split(" ").length != 2){
                    System.out.println("Unrecognised parameters");
                    break;
                }
                dbName = command.split(" ")[1];
                try{
                    db = new DataBase(dbName);
                    System.out.println("Database " + dbName + " created");
                }
                catch (IOException e){
                    System.out.println("Database already exists!");
                }
                break;
            case 'o':
                if(command.split(" ").length != 2){
                    System.out.println("Unrecognised parameters");
                    break;
                }
                dbName = command.split(" ")[1];
                File dbFile = new File(dbName);
                if(!dbFile.exists()){
                    System.out.println("File does not exist");
                    break;
                }
                try{
                    db = new DataBase(dbFile);
                    System.out.println("Database " + dbName + " opened");
                }
                catch (IOException e){
                    e.printStackTrace();
                    db = null;
                    break;
                }
                break;
            case 'f':
                if(command.split(" ").length != 2){
                    System.out.println("Unrecognised parameters");
                    break;
                }
                File commandsFile = new File(command.split(" ")[1]);
                if(!commandsFile.exists()){
                    System.out.println("File does not exist");
                    break;
                }
                executeCommandsFromFile(commandsFile);
                break;
        }
        return true;
    }

    public static void executeCommandsFromFile(File file){
        try(BufferedReader reader = new BufferedReader(new FileReader(file))){
            String command;
            while ((command = reader.readLine()) != null)
                executeCommand(command);
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

}
