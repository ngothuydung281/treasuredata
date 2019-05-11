package coding.exercise;

import com.treasuredata.client.*;
import org.apache.commons.cli.*;
import java.util.ArrayList;
import java.util.List;

public class MyExercise {
    public static List<String> errorList = new ArrayList<>();
    public static TDClient client;

    public static void main(String[] args) {
        System.out.println("===== Start processing =======");

        try {
            client = TDClient.newBuilder()
                    .setApiKey("10574/df5e23d8147a928c46adb1bc5aa2083cdaed5117")
                    .build();

            Option.initCommandOption();
            new ProcessCommandLine(args).process();
        }
        catch (Exception ex) {
            System.out.println("Error while trying to execute command. Please make sure command line arguments are correct" + ex.toString());
            help();
            closeClientConnection();
            System.exit(1);
        }
        finally {
            closeClientConnection();
            if (!errorList.isEmpty())
                System.exit(1);
            else
                System.exit(0);
        }
    }

    public static void closeClientConnection() {
        System.out.print("===== End processing =======");
        if (client != null) {
            client.close();
        }
    }

    public static void help() {
        System.out.println("Example arguments: -f csv -e hive -c 'my_col1,my_col2,my_col5' -m 1427347140 -M 1427350725 -l 100 my_db my_table");
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("Main", Option.options);
    }
}
