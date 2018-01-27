package log.tpc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;

public class DS {
    public static void main( String[] args ) {

        Path fin = Paths.get("../log_tpcds_bin_partitioned_orc_2");
        Path fout = Paths.get("./log_tpcds_2_shuffle");

        try {
            Files.delete(fout);
        } catch (NoSuchFileException x) {
            System.err.format("%s: no such" + " file or directory%n", fout);
        } catch (DirectoryNotEmptyException x) {
            System.err.format("%s not empty%n", fout);
        } catch (IOException x) {
            // File permission problems are caught here.
            System.err.format("IOException: %s%n", x);
        }

        Charset charset = Charset.forName("US-ASCII");

        try (BufferedReader reader = Files.newBufferedReader(fin, charset);
             BufferedWriter writer = Files.newBufferedWriter(fout, charset)) {

            String line = null;
            while ((line = reader.readLine()) != null) {

                int count = line.indexOf(line.trim());

                if (line.contains("Data size")) {
                    writer.append(line.substring(0, count) + "Data size: ");
                    String data_size = line.substring(line.indexOf("Data size:") + "Data size:".length()).trim().split(" ")[0];
                    writer.append(String.valueOf((int) (Long.valueOf(data_size) / (1024 * 1024.0))));
                    writer.newLine();
                    continue;
                }

                if (count > 6) {
                    assert (!line.contains("Operator Tree"));
                    continue;
                }

                // writer.append(String.valueOf(count));

                writer.append(line);
                writer.newLine();
            }

        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }
        // input scanned

        // get largest map->reduce:
        // last line of *more than one* line of "Map Operator Tree"

        Path fi = fout;
        Path fo = Paths.get("./log_tpcds_2_shuffle_largest");

        try {
            Files.delete(fo);
        } catch (NoSuchFileException x) {
            System.err.format("%s: no such" + " file or directory%n", fo);
        } catch (DirectoryNotEmptyException x) {
            System.err.format("%s not empty%n", fo);
        } catch (IOException x) {
            // File permission problems are caught here.
            System.err.format("IOException: %s%n", x);
        }

        try (BufferedReader reader = Files.newBufferedReader(fi, charset);
             BufferedWriter writer = Files.newBufferedWriter(fo, charset)) {

            String line = null;
            while ((line = reader.readLine()) != null) {

                if (line.contains("query")) {
                    writer.newLine();
                    writer.append(line);
                    writer.newLine();
                    continue;
                }
                // for lines that do not contain query

                if (line.contains("Map Operator Tree:")) {
                    // writer.append(line);
                    // writer.newLine();

                    int count = 0;
                    String temp = null;
                    while ((line = reader.readLine()) != null) {
                        if (line.contains("Data size:")) {
                            temp = line;
                            count += 1;
                        } else {
                            break;
                        }
                    }

                    if (temp != null && count > 1) {
                        writer.append(temp);
                        writer.newLine();
                    }
                }
            }

        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }

        // if more than one line, cannot use the heuristic
        Path fii = fo;
        Path foo = Paths.get("./log_tpcds_2_shuffle_largest_h");

        try {
            Files.delete(foo);
        } catch (NoSuchFileException x) {
            System.err.format("%s: no such" + " file or directory%n", foo);
        } catch (DirectoryNotEmptyException x) {
            System.err.format("%s not empty%n", foo);
        } catch (IOException x) {
            // File permission problems are caught here.
            System.err.format("IOException: %s%n", x);
        }

        try (BufferedReader reader = Files.newBufferedReader(fii, charset);
             BufferedWriter writer = Files.newBufferedWriter(foo, charset)) {

            String line = null;
            while ((line = reader.readLine()) != null) {

                if (line.contains("query")) {
                    String query = line;

                    int count = 0;
                    String temp = null;
                    int largest = Integer.MIN_VALUE;

                    while ((line = reader.readLine()) != null) {
                        if (line.contains("Data size:")) {
                            temp = line;
                            count += 1;

                            int cur = Integer.valueOf(line.trim().split(" ")[2]);
                            largest = Integer.max(largest, cur);
                        }
                        if (line.equals("")) {
                            break;
                        }
                    }

                    // if (count == 1) {

                        writer.append(query);
                        writer.newLine();

                        writer.append(String.valueOf(largest));
                        writer.newLine();
                    // }
                 }
            }

        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }


    }
}
