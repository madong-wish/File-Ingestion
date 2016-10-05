import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by manduo dong on 10/3/16.
 *
 */
public class WriteToFile implements Runnable {
    private ArrayList<String> partition;
    private int index;
    private int count;
    private String fileName;

    WriteToFile(ArrayList<String> partition, String fileName, int index, int count) {
        this.partition = partition;
        this.index = index;
        this.count = count;
        this.fileName = fileName;
    }

    @Override
    public void run() {
        //long startTime = System.currentTimeMillis();
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
                    String.format("data/%s%s%d%s%d%s", fileName, ".", index, ".", count, ".txt"))));

            for (String line : partition) {
                writer.append(line);
                writer.newLine();
            }
            writer.close();
        }
        catch (IOException e) {
            //suppress exception
        }
        partition.clear();
        //System.out.println(String.format("write %d_%d execute time: %d", index, count, System.currentTimeMillis() - startTime));
    }
}
