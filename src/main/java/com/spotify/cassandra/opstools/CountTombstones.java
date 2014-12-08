/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.spotify.cassandra.opstools;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Counts the number of tombstones in a SSTable
 */
public class CountTombstones {

  /**
   * Counts the number of tombstones, per row, in a given SSTable
   *
   * Assumes RandomPartitioner, standard columns and UTF8 encoded row keys
   *
   * Does not require a cassandra.yaml file or system tables.
   *
   * @param args command lines arguments
   *
   * @throws java.io.IOException on failure to open/read/write files or output streams
   */
  public static void main(String[] args) throws IOException, ParseException {
    String usage = String.format("Usage: %s [-l] <sstable> [<sstable> ...]%n", CountTombstones.class.getName());

    final Options options = new Options();
    options.addOption("l", "legend", false, "Include column name explanation");
    options.addOption("p", "partitioner", true, "The partitioner used by database");

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.getArgs().length < 1)
    {
      System.err.println("You must supply at least one sstable");
      System.err.println(usage);
      System.exit(1);
    }

    // Fake DatabaseDescriptor settings so we don't have to load cassandra.yaml etc
    Config.setClientMode(true);
    String partitionerName = String.format("org.apache.cassandra.dht.%s",
                                           options.hasOption("p") ? options.getOption("p") : "RandomPartitioner");
    try {
      Class<?> clazz = Class.forName(partitionerName);
      IPartitioner partitioner = (IPartitioner) clazz.newInstance();
      DatabaseDescriptor.setPartitioner(partitioner);
    } catch (Exception e) {
      throw new RuntimeException("Can't instantiate partitioner " + partitionerName);
    }

    PrintStream out = System.out;

    for (String arg : cmd.getArgs()) {
      String ssTableFileName = new File(arg).getAbsolutePath();

      Descriptor descriptor = Descriptor.fromFilename(ssTableFileName);

      run(descriptor, cmd, out);
    }

    System.exit(0);
  }

  private static void run(Descriptor desc, CommandLine cmd, PrintStream out) throws IOException {
    // Since we don't have a schema, make one up!
    CFMetaData cfm = new CFMetaData(desc.ksname, desc.cfname, ColumnFamilyType.Standard,
                                    UTF8Type.instance, UTF8Type.instance);

    SSTableReader reader = SSTableReader.open(desc, cfm);
    SSTableScanner scanner = reader.getScanner();

    long totalTombstones = 0, totalColumns = 0;
    if (cmd.hasOption("l")) {
      out.printf(desc.baseFilename() + "\n");
      out.printf("rowkey #tombstones (#columns)\n");
    }
    while (scanner.hasNext()) {
      SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();

      int tombstonesCount = 0, columnsCount = 0;
      while (row.hasNext())
      {
        OnDiskAtom column = row.next();
        long now = System.currentTimeMillis();
        if (column instanceof Column && ((Column) column).isMarkedForDelete(now)) {
          tombstonesCount++;
        }
        columnsCount++;
      }
      totalTombstones += tombstonesCount;
      totalColumns += columnsCount;

      if (tombstonesCount > 0) {
        String key;
        try {
          key = UTF8Type.instance.getString(row.getKey().key);
        } catch (RuntimeException e) {
          key = BytesType.instance.getString(row.getKey().key);
        }
        out.printf("%s %d (%d)%n", key, tombstonesCount, columnsCount);
      }

    }

    if (cmd.hasOption("l")) {
      out.printf("#total_tombstones (#total_columns)\n");
    }
    out.printf("%d (%d)%n", totalTombstones, totalColumns);

    scanner.close();
  }

}
