package org.rocksdb.crash;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import picocli.CommandLine;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@CommandLine.Command(name="crash")
public class RocksDBCrash  {

  @CommandLine.Option(names={"-p", "--path"}, description = "Path to the folder where the DBs will be located.")
  private String path;

  @CommandLine.Option(names={"-n", "--number-of-db"}, description = "Number of Databases to use.")
  private int DBs;

  @CommandLine.Option(names={"-k","--number-of-keys"}, description = "Number of keys to write per DB.")
  private int keys;

  @CommandLine.Option(names={"-s", "--skip-keys"}, description = "Skip creating keys.")
  private boolean skipKeys;

  @CommandLine.Option(names={"-o", "--number-of-closes"}, description = "Number of open and closes in all.")
  private int iterations;

  @CommandLine.Option(names={"-c", "--concurrency"}, description = "Number of concurrent operations.")
  private int threads;

  private CommandLine cmd;
  public RocksDBCrash() {
    cmd = new CommandLine(this);
  }
  public Integer call() throws Exception {
    run(path, DBs, keys, skipKeys, iterations, threads);
    return null;
  }

  public static void run(String path,
                         int DBs,
                         int keys,
                         boolean skipKeys,
                         int iterations,
                         int threads) throws InterruptedException {
    File rootPath = new File(path);
    rootPath.mkdirs();
    final AtomicInteger count = new AtomicInteger();
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger collided = new AtomicInteger();

    ExecutorService executorService = Executors.newFixedThreadPool(threads);
    for (int i=0;i <iterations; i++) {
      int runId = i;
      executorService.execute(() -> {
        int cont = new Random().nextInt(DBs);
        File containerDir = new File(rootPath, "cont-" + cont);
        DBOptions options = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        WriteOptions writeOptions = new WriteOptions().setSync(false);
        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
            new ArrayList<>();
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
            "default".getBytes(StandardCharsets.UTF_8),
            columnFamilyOptions));
        RocksDB db = null;
        try {
          db = RocksDB.open(options, containerDir.getAbsolutePath(),
              columnFamilyDescriptors, columnFamilyHandles);
          opened.incrementAndGet();
          if (!skipKeys) {
            Random random = new Random();
            for (int j = 0; j < keys; j++) {
              String key = "run" + runId + "key" + random.nextInt();
              try {
                db.put(key.getBytes(), key.getBytes());
              } catch (RocksDBException e) {
                e.printStackTrace();
              }
            }
          }
        } catch (Exception e) {
          collided.incrementAndGet();
        }



        try {
          if (db != null) {
            db.pauseBackgroundWork();
          }
          if (db != null) {
            db.close();
          }

          if (writeOptions != null) {
            writeOptions.close();
          }

          if (options != null) {
            options.close();
          }

          for (ColumnFamilyDescriptor c : columnFamilyDescriptors) {
            c.getOptions().close();
          }
          System.out.println("Done with "+count.incrementAndGet());
        } catch (Exception e) {
          System.out.println("Hit exception during pause background work " + e);

        }
      });
    };
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.DAYS);
    System.out.println("Done open:" + opened.get() +" collided:"+collided.get());
  }

  public static void main(String... args) throws Exception {
    RocksDB.loadLibrary();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, 1024);
    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_LOCK_STRIPES, 1024);
    ContainerCache cache = ContainerCache.getInstance(conf);
    cache.clear();
    new RocksDBCrash().execute(args);
    System.exit(0);
  }

  private void execute(String[] args) throws Exception {
    cmd.parseArgs(args);
    call();
  }
}
