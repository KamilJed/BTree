import database.DataBase;
import records.Record;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

public class appMain {
    private static DataBase db;
    
    public static void main(String[] args) {
        printHelp();
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
                db.addToDataBase(new Record(id, pA, pB, pUnion));
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
                    System.out.println("Record deleted " + id);
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
            case 'h':
                printHelp();
                break;
            case 'e':
                if(db == null){
                    System.out.println("Choose or create a database first");
                    break;
                }
                System.out.println("Memory usage - " + db.getMemoryUsage() + "%");
                break;
            case 'w':
                if(command.split(" ").length != 4){
                    System.out.println("Unrecognised parameters");
                    break;
                }
                dbName = command.split(" ")[1];
                id = Integer.parseInt(command.split(" ")[2]);
                pA = Double.parseDouble(command.split(" ")[3]);
                genTestFile(dbName, id, pA);
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

    public static void printHelp(){
        System.out.println("i [name] - create a database");
        System.out.println("o [name] - open a database");
        System.out.println("a [index pA pB pUnion] - add record to database");
        System.out.println("d [index] - delete record from database");
        System.out.println("u [index newIndex pA pB pUnion] - update record in database");
        System.out.println("g [index] - get record from database");
        System.out.println("p - print index as b tree");
        System.out.println("s - print sorted records");
        System.out.println("r - print raw database file");
        System.out.println("t - print raw index file");
        System.out.println("f [fileName] - execute operations from file");
        System.out.println("h - print this message");
        System.out.println("q - quit");
    }

    public static void genTestFile(String fileName, int numOfRecords, double alpha){
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))){
            ArrayList<Integer> indexes = new ArrayList<>();
            Random random = new Random();
            for(int i = 0; i < numOfRecords; i++){
                Record r = new Record();
                writer.write("a " + r.toString());
                writer.newLine();
                if(random.nextDouble() < alpha)
                    indexes.add(r.getIndex());
            }
            for(int id : indexes){
                writer.write("d " + id);
                writer.newLine();
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}
