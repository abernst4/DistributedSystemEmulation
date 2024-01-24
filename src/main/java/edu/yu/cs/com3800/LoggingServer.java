package edu.yu.cs.com3800;

import java.io.File;
import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public interface LoggingServer {

    public static LocalTime timestamp = null;
    //public static String time = SingleTon.getTime();  
    public static String dir = "logs-" + SingleTon.getTime(); 

    default Logger initializeLogging(String fileNamePreface) throws IOException {
        Logger logger = Logger.getLogger(fileNamePreface); 
                
        logger.setLevel(Level.ALL);
        //first create the directory if it's not created
        //String time = SingleTon.getTime(); 
        File logDir = new File(dir + File.separator); 
        if (!logDir.exists()) logDir.mkdirs(); 
        //we have to make the file that puts  
        FileHandler handler = new FileHandler(dir + File.separator +fileNamePreface+".log", true); 
        logger.addHandler(handler);
        SimpleFormatter formatter = new SimpleFormatter(); 
        handler.setFormatter(formatter);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            handler.close();
        })); 
        return logger; 
        //return initializeLogging(fileNamePreface,false);
    }
    default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) throws IOException {
          //.....
        return createLogger(null, fileNamePreface,disableParentHandlers);
    }

    static Logger createLogger(String loggerName, String fileNamePreface, boolean disableParentHandlers) throws IOException {
          //...........
          return null; 
    }

}

class SingleTon{
    private static LocalTime time = LocalTime.now(); 
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("hh:mm:ss"); 
    private static String formattedTime = time.format(formatter); 

    public static String getTime(){
        return formattedTime; 
    }
}