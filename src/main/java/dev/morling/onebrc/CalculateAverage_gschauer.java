/*
 *  Copyright 2023 The original authors
 *
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
package dev.morling.onebrc;

import static java.util.stream.Collectors.joining;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;

/**
 * The goal of this implementation is to be reasonably fast, while keeping it easy to read.
 * It should not contain anything that cannot be found in a "typical enterprise batch", such as:
 * <ul>
 *   <li>algorithmic operations, which the majority of engineers are not aware of,</li>
 *   <li>features from Java 9 and newer, and</li>
 *   <li>"advanced" features like sun.misc.Unsafe.</li>
 * </ul>
 *
 * The resulting solution has 2^8 lines of code, which is considered to be an additional bonus. :-)
 */
public class CalculateAverage_gschauer {

    public static final int MAX_STATIONS = 600;
    public static final int MAX_NAME_LEN = 30;
    public static final int MAX_TEMP_LEN = 5;
    public static final int MAX_LINE_LEN = MAX_NAME_LEN + MAX_TEMP_LEN + 2;

    private static final String FILE = "./measurements.txt";

    private static final int PART_SIZE_MB = 10 * 1048576;
    private static final long FILE_SIZE = new File(FILE).length();

    private static final Map<Integer, Station> stationByHash = new ConcurrentHashMap<>(MAX_STATIONS);

    public static void main(String... args) throws IOException {
        int parts = (int) Math.ceil((double) FILE_SIZE / PART_SIZE_MB);
        LongStream.range(0, parts)
                .mapToObj(i -> i * PART_SIZE_MB)
                .parallel()
                .forEach(CalculateAverage_gschauer::processPart);

        System.out.println(calcStats());
    }

    /**
     * Process a fixed-size part of the file beginning at the given position.<br/>
     * Then, it merges the results into the global station map.
     */
    static void processPart(long posStart) {
        long posEnd = Math.min(FILE_SIZE, posStart + PART_SIZE_MB + MAX_LINE_LEN);
        try (FileChannel channel = FileChannel.open(Paths.get(FILE))) {
            Map<Integer, Station> m = readPart(channel.map(FileChannel.MapMode.READ_ONLY, posStart, posEnd - posStart));
            for (Map.Entry<Integer, Station> e : m.entrySet()) {
                stationByHash.merge(e.getKey(), e.getValue(), Station::merge);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Concatenate a JSON-like representation of the stations with min/avg/max temperature.
     * <p>
     * Note that this method allocates a large string at once.
     * It feels odd to repackage it into a different data structure.
     * Maybe the stations should be written to a BufferedOutputStream instead.
     */
    static String calcStats() {
        return stationByHash.values().stream()
                .parallel()
                .sorted(Comparator.comparing(o -> o.name))
                .map(Object::toString)
                .collect(joining(", ", "{", "}"));
    }

    /**
     * Read lines from the buffer and calculate the min, max, and average temperature for each station.
     */
    static Map<Integer, Station> readPart(MappedByteBuffer buf) {
        HashMap<Integer, Station> m = new HashMap<>(MAX_STATIONS);
        byte[] dest = new byte[MAX_NAME_LEN];
        seekBegin(buf);

        while (buf.position() < (long) CalculateAverage_gschauer.PART_SIZE_MB && buf.remaining() > 5) {
            /*
             * The operation "new String(bytes, 0, len)" needs to be avoided because it either performs
             * "Arrays.copyOfRange()" or does sophisticated character decoding for non-latin strings.
             * Instead, the byte array is hashed to an integer, which is used as a key in the map.
             *
             * In this way, string allocation is done only once per unique station name per part.
             * This could be optimized as well, but since there are only approx. 400 stations...
             */
            int start = buf.position();
            int hash = hashString(buf, dest);
            int idx = buf.position() - start - 1;

            int temp = calcTemp(buf);
            Station slice = m.get(hash);
            if (slice != null) {
                slice.min = Math.min(slice.min, temp);
                slice.max = Math.max(slice.max, temp);
                slice.sum += temp;
                slice.n++;
            }
            else {
                // The station occurred for the first time in this part, so we need to allocate a new entry.
                String name = new String(dest, 0, idx);
                m.put(hash, new Station(name, temp, temp, temp, 1));
            }
        }
        return m;
    }

    /**
     * Calculate the hash of the station name.
     * <p>
     * Implementation note: The station name is written to the heap byte array.
     */
    private static int hashString(ByteBuffer buf, byte[] hb) {
        byte b;
        int hash = 0;
        for (int idx = 0; idx < hb.length && ((b = buf.get()) != ';'); idx++) {
            hash = 31 * hash + b;
            hb[idx] = b;
        }
        return hash;
    }

    /**
     * Calculate the temperature by reading bytes from the buffer.
     * <p>
     * Note that this method has some optimization potential, e.g. by using
     * <ul>
     *   <li>loop unrolling,</li>
     *   <li>bit operations, or</li>
     *   <li>vector operations.</li>
     * </ul>
     */
    private static int calcTemp(MappedByteBuffer buf) {
        int temp = 0, neg = 1;
        byte b = buf.get();
        if (b == '-') {
            neg = -1;
            b = buf.get();
        }

        do {
            if (b != '.') {
                temp = temp * 10 + b - '0';
            }
        } while ((b = buf.get()) != '\n');
        return temp * neg;
    }

    /**
     * Skip to the beginning of the next line.
     * <p>
     * Implementation note: This method is not exactly correct.
     * There an edge case where a station name is a composite name, e.g., Petropavlovsk-Kamchatsky.
     * The solution would be to skip the first line, except at the beginning of the file.
     */
    private static void seekBegin(MappedByteBuffer byteBuffer) {
        if (!Character.isUpperCase(byteBuffer.get(0))) {
            while (byteBuffer.get() != '\n')
                ;
        }
    }

    private static class Station {
        private long sum;
        private int min;
        private int max;
        private int n;
        private final String name;

        Station(String name, int min, int max, long sum, int n) {
            this.name = name;
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.n = n;
        }

        Station merge(Station other) {
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.n += other.n;
            return this;
        }

        public String toString() {
            double avg = Math.round((double) sum / (double) n) / 10.0;
            return String.format("%s=%.1f/%.1f/%.1f", name, min / 10.0, avg, max / 10.0);
        }
    }
}
