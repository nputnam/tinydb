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
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class RegionFileTest {

    private static final Logger log = LogManager.getLogger(RegionFileTest.class);

    @Test
    public void testSimplePut() throws Exception {
        RegionFile region = RegionManager.INSTANCE.createRegion();
        byte[] key = UUID.randomUUID().toString().getBytes();
        try {
            region.add(new Entity(key, "value".getBytes(), System.currentTimeMillis(), false));
            Iterator<Entity> values = RegionManager.INSTANCE.getRegion(key).getValues();
            while (values.hasNext()) {
                Entity next = values.next();
                Assert.assertEquals(new String(key), new String(next.getKey()));
            }
        } finally {
            RegionManager.INSTANCE.destroyAllRegions();
        }
    }

    @Test
    public void testSimpleMultiValueFlush() throws Exception {
        RegionFile region = RegionManager.INSTANCE.createRegion();

        byte[] key1 = UUID.randomUUID().toString().getBytes();
        byte[] key2 = UUID.randomUUID().toString().getBytes();
        List<byte[]> keys = ImmutableList.of(key1, key2);
        try {
            for (byte[] key : keys) {
                region.add(new Entity(key, UUID.randomUUID().toString().getBytes(),System.currentTimeMillis(), false));
            }
            RegionManager.INSTANCE.flushRegion(region);
            for (byte[] key : keys) {
                RegionFile foundRegion = RegionManager.INSTANCE.getRegion(key);
                Optional<Entity> entityOptional = foundRegion.get(key);
                Assert.assertTrue(entityOptional.isPresent());
            }

        } finally {
            RegionManager.INSTANCE.destroyAllRegions();
        }
    }

    @Test
    public void testFlushOrder() throws Exception {
        RegionFile region = RegionManager.INSTANCE.createRegion();

        region.add(new Entity("d".getBytes(), UUID.randomUUID().toString().getBytes(),System.currentTimeMillis(), false));
        region.add(new Entity("a".getBytes(), UUID.randomUUID().toString().getBytes(),System.currentTimeMillis(), false));

        RegionManager.INSTANCE.flushRegion(region);

        region = RegionManager.INSTANCE.getRegion("c".getBytes());

        region.add(new Entity("b".getBytes(), UUID.randomUUID().toString().getBytes(),System.currentTimeMillis(), false));
        region.add(new Entity("c".getBytes(), UUID.randomUUID().toString().getBytes(),System.currentTimeMillis(), false));

        Iterator<Entity> values = region.getValues();
        Entity previous = null;
        while (values.hasNext()) {
            Entity next = values.next();
            if (previous != null) {
             Assert.assertTrue(SignedBytes.lexicographicalComparator().compare(previous.getKey(), next.getKey()) < 0);
            }
            log.info(new String(next.getKey()));
            previous = next;
        }

        RegionManager.INSTANCE.destroyAllRegions();
    }

    @Test
    public void testBunchOPut() throws Exception {
        RegionFile region = RegionManager.INSTANCE.createRegion();

        long start = System.currentTimeMillis();
        byte[] key = null;
        for (int i=0; i<100000; i++) {
            key = UUID.randomUUID().toString().getBytes();
            region.add(new Entity(key, UUID.randomUUID().toString().getBytes(),System.currentTimeMillis(), false));
        }
        log.info("Finished insert in " + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        region = RegionManager.INSTANCE.flushRegion(region);
        log.info("Finished flush in " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        Iterator<Entity> values = region.getValues();
        int count =0;
        Entity previous = null;
        while (values.hasNext()) {
            Entity next = values.next();
            if (previous != null) {
                Assert.assertTrue(SignedBytes.lexicographicalComparator().compare(previous.getKey(), next.getKey()) < 0);
            }
            previous = next;
            count++;
        }
        log.info("Finished iteration of "+count+" records in "+(System.currentTimeMillis() - start));
        RegionManager.INSTANCE.destroyAllRegions();
    }

}
