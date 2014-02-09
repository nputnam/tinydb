package com.woot.region;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.SignedBytes;
import com.woot.storage.Entity;
import com.woot.storage.region.RegionManager;
import com.woot.storage.region.RegionFile;
import junit.framework.Assert;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class RegionFileTest {

    private static final Logger log = LogManager.getLogger(RegionFileTest.class);

    private static RegionManager regionManager = new RegionManager(System.getProperty("java.io.tmpdir") + File.separator + UUID.randomUUID());

    @BeforeClass
    public static void startMananger() throws Exception {
        regionManager.startAsync().awaitRunning();
    }

    @Test
    public void testSimplePut() throws Exception {
        byte[] key = UUID.randomUUID().toString().getBytes();
        try {
            regionManager.put(new Entity(key, "value".getBytes(), System.currentTimeMillis(), false));
            Iterator<Entity> values = regionManager.getRegion(key).get().getValues();
            while (values.hasNext()) {
                Entity next = values.next();
                Assert.assertEquals(new String(key), new String(next.getKey()));
            }
        } finally {
            regionManager.destroyAllRegions();
        }
    }

    @Test
    public void testSimpleMultiValueFlush() throws Exception {

        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        List<byte[]> keys = ImmutableList.of(key1, key2);
        try {
            for (byte[] key : keys) {
                regionManager.put(new Entity(key, UUID.randomUUID().toString().getBytes(), System.currentTimeMillis(), false));
            }
            regionManager.flushRegion(regionManager.getRegion(key1).get());
            for (byte[] key : keys) {
                RegionFile foundRegion = regionManager.getRegion(key).get();
                Optional<Entity> entityOptional = foundRegion.get(key);
                Assert.assertTrue(entityOptional.isPresent());
            }
        } finally {
            regionManager.destroyAllRegions();
        }
    }

    @Test
    public void testFlushOrder() throws Exception {

        regionManager.put(new Entity("d".getBytes(), UUID.randomUUID().toString().getBytes(), System.currentTimeMillis(), false));
        regionManager.put(new Entity("a".getBytes(), UUID.randomUUID().toString().getBytes(), System.currentTimeMillis(), false));
        regionManager.put(new Entity("b".getBytes(), UUID.randomUUID().toString().getBytes(), System.currentTimeMillis(), false));
        regionManager.put(new Entity("c".getBytes(), UUID.randomUUID().toString().getBytes(), System.currentTimeMillis(), false));

        Optional<RegionFile> region = regionManager.getRegion("c".getBytes());

        Iterator<Entity> values = region.get().getValues();
        Entity previous = null;
        while (values.hasNext()) {
            Entity next = values.next();
            if (previous != null) {
                Assert.assertTrue(SignedBytes.lexicographicalComparator().compare(previous.getKey(), next.getKey()) < 0);
            }
            log.info(new String(next.getKey()));
            previous = next;
        }

        regionManager.destroyAllRegions();
    }

    @Test
    public void testBunchOPut() throws Exception {

        long start = System.currentTimeMillis();
        byte[] key = null;
        for (int i = 0; i < 1000; i++) {
            key = UUID.randomUUID().toString().getBytes();
            regionManager.put(new Entity(key, UUID.randomUUID().toString().getBytes(), System.currentTimeMillis(), false));
        }
        log.info("Finished insert in " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        Iterator<Entity> values = regionManager.getRegion(key).get().getValues();
        int count = 0;
        Entity previous = null;
        while (values.hasNext()) {
            Entity next = values.next();
            if (previous != null) {
                Assert.assertTrue(SignedBytes.lexicographicalComparator().compare(previous.getKey(), next.getKey()) < 0);
            }
            previous = next;
            count++;
        }
        log.info("Finished iteration of " + count + " records in " + (System.currentTimeMillis() - start));
        regionManager.destroyAllRegions();
    }

    @Test
    public void testNewestElement() throws Exception {

        byte[] key = UUID.randomUUID().toString().getBytes();
        regionManager.put(new Entity(key, "test1".getBytes(), 0l, false));
        regionManager.put(new Entity(key, "test2".getBytes(), 1l, false));

        RegionFile lookedUpRegion = regionManager.getRegion(key).get();
        Optional<Entity> entityOptional = lookedUpRegion.get(key);
        Assert.assertTrue(entityOptional.isPresent());
        Assert.assertEquals("test2", new String(entityOptional.get().getValue()));

        regionManager.destroyAllRegions();
    }

    @Test
    public void testNewestElementAfter() throws Exception {
        byte[] key = UUID.randomUUID().toString().getBytes();
        regionManager.put(new Entity(key, "test1".getBytes(), 2l, false));
       regionManager.put(new Entity(key, "test2".getBytes(), 1l, false));

        RegionFile lookedupRegion = regionManager.getRegion(key).get();
        Optional<Entity> entityOptional = lookedupRegion.get(key);
        Assert.assertTrue(entityOptional.isPresent());
        Assert.assertEquals("test1", new String(entityOptional.get().getValue()));

        regionManager.destroyAllRegions();
    }

    @Test
    public void testMultipleSameKeyReturnsOneElement() throws Exception {
        byte[] key = UUID.randomUUID().toString().getBytes();
        regionManager.put(new Entity(key, "test1".getBytes(), 1l, false));
        regionManager.put(new Entity(key, "test2".getBytes(), 2l, false));

        RegionFile lookedupRegion = regionManager.getRegion(key).get();

        Iterator<Entity> values = lookedupRegion.getValues();
        int counter = 0;
        while (values.hasNext()) {
            Entity next = values.next();
            Assert.assertEquals("test2", new String(next.getValue()));
            counter++;
        }
        Assert.assertEquals(1, counter);

        regionManager.destroyAllRegions();
    }

}
