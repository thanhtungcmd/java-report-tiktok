package source.helper;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectDb {

    private static final String CFG_FILE = "config.cfg";
    public static String MONGO_HOST = "127.0.0.1";
    public static int MONGO_PORT = 27017;
    //public static String MONGO_DATABASE = "htv_ditech_test";
    public static String MONGO_DATABASE = "mautic_data";

    public static void loadProperties() {
        FileInputStream file = null;
        String str;
        try {
            if(!new File(CFG_FILE).exists())
                return;
            file = new FileInputStream(CFG_FILE);
            Properties prop = new Properties();
            prop.load(file);
            file.close();

            MONGO_HOST = prop.getProperty("MONGO_HOST", MONGO_HOST);
            MONGO_PORT = Integer.parseInt(prop.getProperty("MONGO_PORT", MONGO_PORT + ""));
            MONGO_DATABASE = prop.getProperty("MONGO_DATABASE", MONGO_DATABASE);
        } catch (Exception e) {
            System.out.println("Error load properties:" + e);
        }
    }

    public static MongoClient createConnection() {
        MongoClient mongoClient =  null;
        try {
            Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
            mongoLogger.setLevel(Level.SEVERE);
            if(mongoClient == null) {
                mongoClient = MongoClients.create("mongodb://"+ MONGO_HOST +":" + MONGO_PORT);
            }
        } catch (Exception e) {
            System.out.println("-- Connect Mongo Fail --");
            e.printStackTrace();
        }

        return mongoClient;
    }

    public static void closeConnection(MongoClient mongoClient) {
        try {
            if (mongoClient != null) {
                mongoClient.close();
                System.out.println("-- Close Mongo Success --");
            }
        } catch (Exception e) {
            System.out.println("-- Close Mongo Fail --");
            e.printStackTrace();
        }
    }

}
