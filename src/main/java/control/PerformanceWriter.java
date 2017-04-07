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

    public static void write(final JobExecutionResult res, final String path) {
        double elapsed = res.getNetRuntime(TimeUnit.NANOSECONDS);
        long tuples = res.getAccumulatorResult("tuples");
        double latency = elapsed / tuples;

        PerformanceWriter.write(path, elapsed, latency);
    }

    public static void write(final String path, final double elapsed, final double latency) {
        String performanceString = String.format("%.6f %.6f", elapsed / 1000000000.0, latency / 1000000000.0);
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
            writer = new FileWriter(file, false);
            writer.write(performanceString);
            writer.close();
        } catch (IOException exc) {
            exc.printStackTrace();
        } finally {
        }
    }

}

