package com.woot.storage.region;

import com.google.common.base.Throwables;
import com.google.common.primitives.SignedBytes;
import com.woot.storage.Entity;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Semaphore;

public enum  RegionManager {
    INSTANCE;

    private Set<RegionFile> regions = new CopyOnWriteArraySet<RegionFile>();
    private final Semaphore transition = new Semaphore(1);

    public void splitRegion(RegionFile regionFile) {
        try {
            transition.acquire();
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw Throwables.propagate(e);
        } finally {
            transition.release();
        }
    }

    public RegionFile getRegion(byte[] key) {
        NavigableMap<byte[], RegionFile> lookup = new TreeMap<byte[], RegionFile>(SignedBytes.lexicographicalComparator());
        for (RegionFile region : regions) {
            lookup.put(region.getStartKey(), region);
        }
        return lookup.floorEntry(key).getValue();
    }


    public RegionFile createRegion() {
        File file = new File("/tmp/" + UUID.randomUUID().toString());
        try {
            transition.acquire();
            file.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException("Error creating new region file. ", e);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw Throwables.propagate(e);
        } finally {
            transition.release();
        }
        RegionFile regionFile = new RegionFile(file);
        regions.add(regionFile);
        return regionFile;
    }

    public RegionFile flushRegion(RegionFile regionFile) {
        File newRegionFile  = new File("/tmp/"+UUID.randomUUID().toString());
        RegionFile flushedRegion = null;
        try {
            transition.acquire();
            if (regionFile.getMemstore().size() > 0) {
                Iterator<Entity> values = regionFile.getValues();
                while (values.hasNext()) {
                    Entity entity = values.next();
                    if (entity.isDeleted()) {
                        continue;
                    }
                    Files.write(newRegionFile.toPath(), entity.toBytes().array(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                }
                flushedRegion = new RegionFile(newRegionFile);
                regions.add(flushedRegion);
            } else {
                newRegionFile.delete();
            }

        } catch (InterruptedException e) {
            throw new RuntimeException("Error flushing region", e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error flushing region", e);
        } catch (IOException e) {
            throw new RuntimeException("Error flushing region", e);
        } finally {
            transition.release();
            destroyRegion(regionFile);
        }
        if (flushedRegion != null) return flushedRegion;
        else return regionFile;
    }

    public void destroyRegion(RegionFile regionFile) {
        try {
            transition.acquire();
            regionFile.getRegionFile().delete();
            regions.remove(regionFile);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw Throwables.propagate(e);
        } finally {
            transition.release();
        }
    }

    public void destroyAllRegions() {
        try {
            transition.acquire();
            for (RegionFile regionFile : regions) {
                regionFile.getRegionFile().delete();
            }
            regions.clear();
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw Throwables.propagate(e);
        } finally {
            transition.release();
        }
    }

}
