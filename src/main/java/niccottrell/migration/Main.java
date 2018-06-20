package niccottrell.migration;

import com.github.mongobee.Mongobee;
import com.github.mongobee.exception.MongobeeException;
import org.apache.commons.cli.*;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class Main {

    public static String MONGO_URI = "mongodb://localhost:27017/";

    public static void main(String[] args) throws MongobeeException, ParseException {

        CommandLineParser parser = new DefaultParser();

        Options cliopt = new Options();
        cliopt.addOption("c", "uri", true, "MongoDB connection details (default 'mongodb://localhost:27017' )");
        cliopt.addOption("d", "database", true, "MongoDB database to update");
        cliopt.addOption(null, "h", false, "Print help only");
        cliopt.addOption("b", "foreground", false, "Build indexes in the foreground (Default: false = background)");

        CommandLine cmd = parser.parse(cliopt, args);

        // automatically generate the help statement
        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("mongobee-demo", cliopt);
            return;
        }

        MONGO_URI = cmd.getOptionValue("c", "mongodb://localhost:27017/");

        String dbName =  cmd.getOptionValue("d");
        if (isBlank(dbName)) throw new RuntimeException("No target database specified");

        Mongobee runner = new Mongobee(MONGO_URI);
        // host must be set if not set in URI
        runner.setDbName(dbName);
        // package to scan for changesets
        runner.setChangeLogsScanPackage("niccottrell.migration");
        //  starts migration changesets
        runner.execute();

    }

}
