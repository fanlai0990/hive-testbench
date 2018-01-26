package log.tpc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;

public class AttMpls {

    /**
     * Delete a file or a directory and its children.
     * @param file The directory to delete.
     * @throws IOException Exception when problem occurs during deleting the directory.
     */
    private static void delete(File file) throws IOException {

        for (File childFile : file.listFiles()) {

            if (childFile.isDirectory()) {
                delete(childFile);
            } else {
                if (!childFile.delete()) {
                    throw new IOException();
                }
            }
        }

        if (!file.delete()) {
            throw new IOException();
        }
    }

    public static void main( String[] args ) {

        File theDir = new File("TPC-DS-AttMpls");

        // delete existing directory
        try {
            // Files.delete(theDir.toPath());
            delete(theDir);
        } catch (NoSuchFileException x) {
            System.err.format("%s: no such" + " file or directory%n", theDir);
        } catch (DirectoryNotEmptyException x) {
            System.err.format("%s not empty%n", theDir);
        } catch (IOException x) {
            // File permission problems are caught here.
            System.err.format("IOException: %s%n", x);
        }

        // if the directory does not exist, create it
        if (!theDir.exists()) {
            System.out.println("creating directory: " + theDir.getName());
            boolean result = false;

            try{
                theDir.mkdir();
                result = true;
            }
            catch(SecurityException se){
                //handle it
            }
            if(result) {
                System.out.println("DIR created");
            }
        }

        Path fin = Paths.get("../Sim-master/data/combined_traces_fb/TPC-DS-AttMpls.txt");
        Path fout = Paths.get("./TPC-DS-AttMpls/log");

        Charset charset = Charset.forName("US-ASCII");

        // find all queries
        SortedSet<Integer> queries = new TreeSet<>();

        try (BufferedReader reader = Files.newBufferedReader(fin, charset);
              BufferedWriter writer = Files.newBufferedWriter(fout, charset)) {

            int count = -1;

            String line = null;
            while ((line = reader.readLine()) != null) {

                if (line.contains("query")) {
                    queries.add(Integer.valueOf(line.substring(line.indexOf("query") + "query".length())));

                    writer.newLine();
                    writer.append(line);
                    writer.newLine();

                    continue;
                }

                if (line.split(" ")[0].equals(line)) {
                    count = Integer.valueOf(line);
                }

                if (count >= 0) {
                    writer.append(line);
                    writer.newLine();
                    count -= 1;
                }
            }

        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }

        System.out.println(queries.size());

        Map<Integer, Integer> largestMap = new HashMap<>();

        Path foo =  Paths.get("./TPC-DS-AttMpls/log_largest");
        try (BufferedWriter Writer = Files.newBufferedWriter(foo, charset)) {

            // iterate all queries, write to new file
            for (Integer q : queries) {

                Path fi = fout;
                Path fo = Paths.get("./TPC-DS-AttMpls/log_query" + q.toString());

                // scan input
                try (BufferedReader reader = Files.newBufferedReader(fi, charset);
                     BufferedWriter writer = Files.newBufferedWriter(fo, charset)) {

                    boolean isQ = false;

                    String line = null;
                    while ((line = reader.readLine()) != null) {

                        if (line.contains("query")) {
                            int qq = Integer.valueOf(line.substring(line.indexOf("query") + "query".length()));
                            if (qq == q) {
                                // output info of this query
                                isQ = true;

                                writer.newLine();
                                writer.append(line);
                                writer.newLine();
                            }
                            continue;
                        }

                        if (line.trim().equals("")) {
                            // end of this query
                            isQ = false;
                        }

                        if (isQ) {
                            writer.append(line);
                            writer.newLine();
                        }
                    }

                } catch (IOException x) {
                    System.err.format("IOException: %s%n", x);
                }
                // scan input finish

                Path fii = fo;
                // output: inplace append largest map->reduce
                int largest = Integer.MIN_VALUE;

                try (BufferedReader reader = Files.newBufferedReader(fii, charset)) {

                    String line = null;
                    while ((line = reader.readLine()) != null) {

                        if (line.contains("Map") && line.contains("Reducer")) {
                            int cur = Integer.valueOf(line.split(" ")[2]);
                            largest = Integer.max(largest, cur);
                        }
                    }

                    Writer.append("query");
                    Writer.append(String.valueOf(q));
                    Writer.newLine();
                    Writer.append(String.valueOf(largest));
                    Writer.newLine();

                    largestMap.put(q, largest);

                } catch (IOException x) {
                    System.err.format("IOException: %s%n", x);
                }

            } // for: iterate over queries

        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }

        // divide to get the scale factor

        Map<Integer, Integer> largestM = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get("./log_tpcds_2_shuffle_largest_h"), charset)) {

            String line = null;
            while ((line = reader.readLine()) != null) {

                if (line.contains("query")) {
                    String query = line.substring(line.indexOf("query") + "query".length(), line.indexOf("."));

                    if ((line = reader.readLine()) != null) {
                        String data_size = line;
                        largestM.put(Integer.valueOf(query), Integer.valueOf(data_size));
                    }
                }
            }

        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }

        for (Map.Entry<Integer, Integer> e: largestM.entrySet()) {
            for (Map.Entry<Integer, Integer> ee: largestMap.entrySet()) {
                if (e.getKey().equals(ee.getKey())) {
                    System.out.println(e.getKey() + ": " + e.getValue() + ", " + ee.getValue());
                    // System.out.println("!!! " + (int) (Double.valueOf(ee.getValue()) / e.getValue()));
                }
            }
        }

        System.out.println();
        for (Map.Entry<Integer, Integer> e: largestM.entrySet()) {
            for (Map.Entry<Integer, Integer> ee: largestMap.entrySet()) {
                if (e.getKey().equals(ee.getKey())) {
                    // System.out.println(e.getKey() + ": " + e.getValue() + ", " + ee.getValue());
                    System.out.println((int) (Double.valueOf(ee.getValue()) / e.getValue()));
                }
            }
        }
    }
}
