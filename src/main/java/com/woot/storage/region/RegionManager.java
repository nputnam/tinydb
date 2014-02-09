package com.woot.storage.region;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.primitives.SignedBytes;
import com.google.common.util.concurrent.AbstractIdleService;
import com.woot.storage.Entity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Semaphore;

public class RegionManager extends AbstractIdleService {

    private static final Logger log = LogManager.getLogger(RegionManager.class);

    private Set<RegionFile> regions = new CopyOnWriteArraySet<RegionFile>();
    private NavigableMap<byte[], RegionFile> lookup = new TreeMap<byte[], RegionFile>(SignedBytes.lexicographicalComparator());

    private final Semaphore transition = new Semaphore(1);

    private final String base;

    public RegionManager(String base) {
        if (base.endsWith(File.separator)) {
            this.base = base;
        } else {
            this.base = base + File.separator;
        }
    }

    public Optional<RegionFile> getRegion(byte[] key) {
        if (regions.isEmpty()) {
            createEmptyRegion();
        }
        if (regions.size() == 1) {
            return Optional.of(regions.iterator().next());
        }
        // Lazy init the lookup for key -> region
        if (lookup.isEmpty()) {
            for (RegionFile region : regions) {
                lookup.put(region.getStartKey(), region);
            }
        }
        NavigableMap<byte[], RegionFile> subMap = lookup.subMap(key, true, key, false);
        if (subMap.size() > 1) {
            log.error("Returned multiple regions for the same key!!! ");
        } else if (subMap.isEmpty()) {
            log.error("No regions for key!!! ");
        }
        return Optional.fromNullable(subMap.firstEntry().getValue());
    }

    private File createRegionFile() {
        lookup.clear();
        return new File(base + UUID.randomUUID().toString());
    }

    public RegionFile createEmptyRegion() {
        File file = createRegionFile();
        try {
            file.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException("Error creating new region file. ", e);
        }
        RegionFile regionFile = new RegionFile(file, this);
        regions.add(regionFile);
        return regionFile;
    }

    public RegionFile flushRegion(RegionFile regionFile) {
        try {
            if (regionFile.getMemstore().size() > 0) {
                transition.acquire();
                Iterator<Entity> values = regionFile.getValues();
                File regionDiskFile = createRegionFile();
                while (values.hasNext()) {
                    Entity entity = values.next();
                    if (entity.isDeleted()) {
                        continue;
                    }
                    Files.write(regionDiskFile.toPath(), entity.toBytes().array(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                }
                RegionFile newRegionFile = new RegionFile(regionDiskFile, this);
                regions.add(newRegionFile);
                destroyRegion(regionFile);
                return newRegionFile;
            }
            return regionFile;
        } catch (IOException e) {
            throw new RuntimeException("Error flushing region", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw Throwables.propagate(e);
        } finally {
            transition.release();
        }
    }

    public void put(Entity entity) {
        Optional<RegionFile> region = getRegion(entity.getKey());
        if (region.isPresent()) {
            region.get().add(entity);
        }
    }

    public void destroyRegion(RegionFile regionFile) {
        regionFile.getRegionFile().delete();
        regions.remove(regionFile);
    }

    public void destroyAllRegions() {

        for (RegionFile regionFile : regions) {
            regionFile.getRegionFile().delete();
        }
        regions.clear();
    }

    @Override
    protected void startUp() throws Exception {
        log.info("Starting region manager...");
        File storageDir = new File(base);
        if (!storageDir.exists()) {
            storageDir.mkdir();
        }
        File[] files = storageDir.listFiles();
        log.info("Initializing "+files.length+" regions.");
        for (File file : files) {
            try {
                RegionFile regionFile = new RegionFile(file, this);
                regions.add(regionFile);
            } catch (Exception e) {
                log.error("Error opening region "+file.toPath().getFileName());
            }
        }
    }

    @Override
    protected void shutDown() throws Exception {
        log.info("Shutting down region manager, flushing " + regions.size() + " regions.");
        for (RegionFile region : regions) {
            flushRegion(region);
        }
    }
}
