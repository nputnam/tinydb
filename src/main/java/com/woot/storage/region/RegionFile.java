package com.woot.storage.region;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.primitives.SignedBytes;
import com.woot.storage.Entity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.*;


/**
 * Class to represent what should be the on disk logical region. A region has :
 * <p/>
 * * A file on disk.
 * * A wall file that contains elements in the memstore that haven't been merged with the main file yet.
 * * A start key for the first element key in the list.
 * * An end key for the last element key in the list.
 */
public class RegionFile {

    private static final Logger log = LogManager.getLogger(RegionFile.class);

    private final File regionFile;
    private final long createdAt = System.currentTimeMillis();
    private byte[] startKey = null;
    private byte[] endKey = null;
    private final Map<byte[], Entity> memstore = new TreeMap<byte[], Entity>(SignedBytes.lexicographicalComparator());

    public RegionFile(File regionFile) {
        this.regionFile = regionFile;
        int numRecords = 0;
        // Open the region file and fill out the stuffs we need.
        Iterator<Entity> diskValues = getDiskValues();
        while (diskValues.hasNext()) {
            Entity next = diskValues.next();
            updateRange(next);
            numRecords++;
        }

        if (startKey != null && endKey != null) {
            log.info(String.format("Region opened (%d). Start key is %s and end key is %s", numRecords, new String(startKey), new String(endKey)));
        } else {
            log.info(String.format("Region %s opened (%d)", regionFile.getAbsolutePath(), numRecords));
        }
    }

    public Map<byte[], Entity> getMemstore() {
        return memstore;
    }

    public void a dd(Entity entity) {
        this.memstore.put(entity.getKey(), entity);
        updateRange(entity);
    }

    public Optional<Entity> get(byte[] key) {
        Iterator<Entity> values = getValues();
        while (values.hasNext()) {
            Entity next = values.next();
            if (Arrays.equals(key, next.getKey())) {
                return Optional.of(next);
            }
        }
        return Optional.absent();
    }

    public File getRegionFile() {
        return regionFile;
    }

    public Iterator<Entity> getValues() {
        ImmutableList<Iterator<Entity>> of = ImmutableList.<Iterator<Entity>>of(memstore.values().iterator(), getDiskValues());
        return Iterators.mergeSorted(of, new EnityComparator());
    }

    private Iterator<Entity> getDiskValues() {
        return new RegionFileIterator(regionFile);
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public void updateRange(Entity entity) {
        if (entity == null) return;
        if (startKey == null) {
            startKey = entity.getKey();
        }
        if (endKey == null) {
            endKey = entity.getKey();
        }

        // Bubble sort and validate the records.
        if (SignedBytes.lexicographicalComparator().compare(entity.getKey(), startKey) < 0) {
            startKey = entity.getKey();
        }

        if (SignedBytes.lexicographicalComparator().compare(entity.getKey(), endKey) > 0) {
            endKey = entity.getKey();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RegionFile that = (RegionFile) o;

        if (createdAt != that.createdAt) return false;
        if (!Arrays.equals(endKey, that.endKey)) return false;
        if (!memstore.equals(that.memstore)) return false;
        if (!regionFile.equals(that.regionFile)) return false;
        if (!Arrays.equals(startKey, that.startKey)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = regionFile.hashCode();
        result = 31 * result + (int) (createdAt ^ (createdAt >>> 32));
        result = 31 * result + (startKey != null ? Arrays.hashCode(startKey) : 0);
        result = 31 * result + (endKey != null ? Arrays.hashCode(endKey) : 0);
        result = 31 * result + memstore.hashCode();
        return result;
    }
}

