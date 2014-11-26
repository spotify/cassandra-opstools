package com.spotify.cassandra.opstools;

import com.google.common.collect.Lists;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMetadata;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Shows the minimum and maximum sstable timestamps
 */
public class SSTableTimestampViewer
{
    /**
     * @param args a list of sstables whose metadata we're interested in
     */
    public static void main(String[] args) throws IOException
    {
        PrintStream out = System.out;
        if (args.length == 0)
        {
            out.println("Usage: spcassandra-sstable-timestamp <sstable filenames>");
            System.exit(1);
        }

        List<TimeMetadata> metadata = Lists.newArrayListWithExpectedSize(args.length);
        for (String fname : args)
        {
            Descriptor descriptor = Descriptor.fromFilename(fname);
            SSTableMetadata md = SSTableMetadata.serializer.deserialize(descriptor).left;
            metadata.add(new TimeMetadata(descriptor.toString(), md.minTimestamp, md.maxTimestamp, new java.io.File(descriptor.baseFilename() + "-Data.db").length()));
        }

        Collections.sort(metadata, new Comparator<TimeMetadata>() {
            public int compare(TimeMetadata o1, TimeMetadata o2) {
                return Long.compare(o1.minTimestamp, o2.minTimestamp);
            }
        });

        long[] timespanHistogram = new long[metadata.size() + 1];
        SortedSet<TimeMetadata> currentOverlaps = new TreeSet<>(new Comparator<TimeMetadata>() {
            public int compare(TimeMetadata o1, TimeMetadata o2) {
                return Long.compare(o1.maxTimestamp, o2.maxTimestamp);
            }
        });

        List<Interval<Long, Integer>> intervals = Lists.newArrayList();

        long currentTime = 0;
        boolean wasMax = false;
        for (TimeMetadata md : metadata)
        {
            while (currentOverlaps.size() > 0 && currentOverlaps.first().maxTimestamp < md.minTimestamp) {
                intervals.add(new Interval<>(currentTime, !wasMax, currentOverlaps.first().maxTimestamp, true, currentOverlaps.size()));
                timespanHistogram[currentOverlaps.size()] += currentOverlaps.first().maxTimestamp - currentTime;
                currentTime = currentOverlaps.first().maxTimestamp;
                wasMax = true;
                currentOverlaps.remove(currentOverlaps.first());
            }
            if (currentTime != 0) {
                intervals.add(new Interval<>(currentTime, !wasMax, md.minTimestamp, false, currentOverlaps.size()));
                timespanHistogram[currentOverlaps.size()] += md.minTimestamp - currentTime;
            }
            currentTime = md.minTimestamp;
            wasMax = false;
            currentOverlaps.add(md);
        }
        while (currentOverlaps.size() > 0) {
            intervals.add(new Interval<>(currentTime, !wasMax, currentOverlaps.first().maxTimestamp, true, currentOverlaps.size()));
            timespanHistogram[currentOverlaps.size()] += currentOverlaps.first().maxTimestamp - currentTime;
            currentTime = currentOverlaps.first().maxTimestamp;
            wasMax = true;
            currentOverlaps.remove(currentOverlaps.first());
        }

        for (TimeMetadata md : metadata)
            out.println(md);

        for (Interval<Long, Integer> interval : intervals)
            out.println(interval);
        out.println();

        for (int i = 0; i < timespanHistogram.length; i++)
            out.printf("Total time covered by %s sstables: %s (%.2f%%)%n", i, timespanHistogram[i], (double)timespanHistogram[i] / (currentTime - metadata.get(0).minTimestamp) * 100);
    }

    private static class TimeMetadata
    {
        public final String name;
        public final long minTimestamp;
        public final long maxTimestamp;
        public final long dataFileSize;

        TimeMetadata(String name, long minTimestamp, long maxTimestamp, long dataFileSize)
        {
            this.name = name;
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
            this.dataFileSize = dataFileSize;
        }

        public String toString()
        {
            return "SSTable: " + name + "\n" +
                   "Data file size (in bytes): " + dataFileSize + "\n" +
                   "Minimum timestamp: " + minTimestamp + "\t" + new Date(minTimestamp / 1000) + "\n" +
                   "Maximum timestamp: " + maxTimestamp + "\t" + new Date(maxTimestamp / 1000) + "\n";
        }
    }

    private static class Bound<T>
    {
        public final T bound;
        public final boolean closed;

        public Bound(T bound, boolean closed)
        {
            this.bound = bound;
            this.closed = closed;
        }
    }

    private static class Interval<T, V>
    {
        public final Bound<T> lowerBound;
        public final Bound<T> upperBound;
        public final V value;

        public Interval(Bound<T> lowerBound, Bound<T> upperBound, V value)
        {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.value = value;
        }

        public Interval(T lowerBound, boolean closedLowerBound, T upperBound, boolean closedUpperBound, V value)
        {
            this.lowerBound = new Bound<T>(lowerBound, closedLowerBound);
            this.upperBound = new Bound<T>(upperBound, closedUpperBound);
            this.value = value;
        }

        public String toString()
        {
            return String.format("In interval %s%s, %s%s: %s sstables",
                                 lowerBound.closed ? "[" : "(", lowerBound.bound, upperBound.bound, upperBound.closed ? "]" : ")", value);
        }
    }
}
