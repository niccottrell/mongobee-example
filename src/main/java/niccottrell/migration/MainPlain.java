package niccottrell.migration;

import com.github.mongobee.Mongobee;
import com.github.mongobee.exception.MongobeeException;

public class MainPlain {

    public static final String MONGO_URI = "mongodb://localhost:27017/";

    public static void main(String[] args) throws MongobeeException {

        Mongobee runner = new Mongobee(MONGO_URI);
        // host must be set if not set in URI
        runner.setDbName("bee1");
        // package to scan for changesets
        runner.setChangeLogsScanPackage("niccottrell.migration");
        //  starts migration changesets
        runner.execute();

    }

}
