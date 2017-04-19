package control;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by maurizio on 04/04/17.
 *
 * Returns information about runtime memory status
 */

public class RuntimeMemoryInfo implements Serializable
{
    private static final long serialVersionUID = 7164627809168103391L;

    public long totalMemory() {
        return Runtime.getRuntime().totalMemory();
    }

    public long freeMemory() {
        return Runtime.getRuntime().freeMemory();
    }

    public long maxMemory() {
        return Runtime.getRuntime().maxMemory();
    }

    public static void writeUsedMemory(int i) {
        //System.gc();
        String performanceString = String.format("usedMemory: %d", Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
        String path = "/Users/maurizio/Desktop/MemoryWithGC" + i + ".txt";
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
