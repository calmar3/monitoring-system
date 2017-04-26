package control;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobExecutionResult;

/**
 * Created by maurizio on 04/04/17.
 */

public class PerformanceWriter {

    public static void write(final JobExecutionResult res, final String path, long tuples, int parallelism) {

        double elapsed = res.getNetRuntime(TimeUnit.NANOSECONDS);
        double latency = elapsed / tuples;
        String performanceString = String.format("parallelism: %d tuples: %d elapsed: %.6f latency: %.6f", parallelism, tuples, elapsed / 1000000000.0, latency / 1000000000.0);

        //System.out.println(performanceString);

        File file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException exc) {
                exc.printStackTrace();
            }
        }

        FileWriter writer;
        try {
            writer = new FileWriter(file, true);
            writer.write("\n" + performanceString);
            writer.close();
        } catch (IOException exc) {
            exc.printStackTrace();
        }
    }

}

