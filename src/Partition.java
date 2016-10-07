import java.io.*;
import java.util.ArrayList;

/**
 * Created by manduo dong on 9/29/16.
 *
 */


public class Partition {
    /**
     * Split file into several chunks
     * @param file name of the file to be split
     * @param splitSize size of each split file
     * @return number of cycles
     */
    int partitionFile(File file, int splitSize, long MAX_LINE_PER_CYCLE) {
        long count = 0;
        int cycle = 0;

        try {
            long startTime = System.currentTimeMillis();
            ArrayList<ArrayList<String>> partitions = new ArrayList<>();
            for (int i = 0; i < splitSize; i++) {
                partitions.add(new ArrayList<>());
            }
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            int index;

            while ((line = br.readLine()) != null) {
                try {
                    index = Integer.parseInt(line.split(",")[0]) % splitSize;
                }
                catch (NumberFormatException e) {
                    continue;   //suppress exception
                }
                partitions.get(index).add(line);

                if (++count > MAX_LINE_PER_CYCLE) {
                    count = 0;
                    for (int i = 0; i < splitSize; i++) {
                        new Thread(new WriteToFile(partitions.get(i), file.getName(), i, cycle)).start();
                    }
                    System.out.println(String.format("Processed %d lines", (cycle + 1) * MAX_LINE_PER_CYCLE));
                    cycle++;
                    partitions = new ArrayList<>();
                    for (int i = 0; i < splitSize; i++) {
                        partitions.add(new ArrayList<>());
                    }
                }
            }

            if (count != 0) {
                for (int i = 0; i < splitSize; i++) {
                    new Thread(new WriteToFile(partitions.get(i), file.getName(), i, cycle)).start();
                }
            }

            br.close();
            ComputeAverageTime.wait = false;

            // uncomment below lines if the original file can be deleted after processing
//            if (!file.delete()) {
//                System.out.println("FAILED to delete file");
//            }
            System.out.println(String.format("Total execute time: %d sec", (System.currentTimeMillis() - startTime)/1000));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cycle;
    }
}
