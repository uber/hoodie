package com.uber.hoodie.bench.reader;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.log.HoodieMergedLogRecordScanner;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieMemoryConfig;
import com.uber.hoodie.func.ParquetReaderIterator;
import com.uber.hoodie.hadoop.realtime.AbstractRealtimeRecordReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * This class helps to generate updates from an already existing hoodie dataset. It supports generating updates in
 * across partitions, files and records.
 */
public class DFSHoodieDatasetInputReader extends DFSDeltaInputReader {

  private static Logger log = LogManager.getLogger(DFSHoodieDatasetInputReader.class);

  private transient JavaSparkContext jsc;
  private String schemaStr;
  private HoodieTableMetaClient metaClient;

  public DFSHoodieDatasetInputReader(JavaSparkContext jsc, String basePath, String schemaStr) {
    this.jsc = jsc;
    this.schemaStr = schemaStr;
    this.metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
  }

  protected List<String> getPartitions(Optional<Integer> partitionsLimit) throws IOException {
    List<String> partitionPaths = FSUtils
        .getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(), false);
    // Sort partition so we can pick last N partitions by default
    Collections.sort(partitionPaths);
    if (!partitionPaths.isEmpty()) {
      Preconditions.checkArgument(partitionPaths.size() >= partitionsLimit.get(),
          "Cannot generate updates for more partitions " + "than present in the dataset, partitions "
              + "requested " + partitionsLimit.get() + ", partitions present " + partitionPaths.size());
      return partitionPaths.subList(0, partitionsLimit.get());
    }
    return partitionPaths;

  }

  private JavaPairRDD<String, Iterator<FileSlice>> getPartitionToFileSlice(HoodieTableMetaClient metaClient,
      List<String> partitionPaths) {
    TableFileSystemView.RealtimeView fileSystemView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants());
    // pass num partitions to another method
    JavaPairRDD<String, Iterator<FileSlice>> partitionToFileSliceList = jsc.parallelize(partitionPaths).mapToPair(p -> {
      return new Tuple2<>(p, fileSystemView.getLatestFileSlices(p).iterator());
    });
    return partitionToFileSliceList;
  }

  @Override
  protected long analyzeSingleFile(String filePath) {
    return SparkBasedReader.readParquet(new SparkSession(jsc.sc()), Arrays.asList(filePath),
        Optional.empty(), Optional.empty()).count();
  }

  private JavaRDD<GenericRecord> fetchAnyRecordsFromDataset(Optional<Long> numRecordsToUpdate) throws IOException {
    return fetchRecordsFromDataset(Optional.empty(), Optional.empty(), numRecordsToUpdate, Optional.empty());
  }

  private JavaRDD<GenericRecord> fetchAnyRecordsFromDataset(Optional<Long> numRecordsToUpdate, Optional<Integer>
      numPartitions) throws IOException {
    return fetchRecordsFromDataset(numPartitions, Optional.empty(), numRecordsToUpdate, Optional.empty());
  }

  private JavaRDD<GenericRecord> fetchPercentageRecordsFromDataset(Optional<Integer> numPartitions, Optional<Integer>
      numFiles, Optional<Double> percentageRecordsPerFile) throws IOException {
    return fetchRecordsFromDataset(numPartitions, numFiles, Optional.empty(), percentageRecordsPerFile);
  }

  private JavaRDD<GenericRecord> fetchRecordsFromDataset(Optional<Integer> numPartitions, Optional<Integer>
      numFiles, Optional<Long> numRecordsToUpdate) throws IOException {
    return fetchRecordsFromDataset(numPartitions, numFiles, numRecordsToUpdate, Optional.empty());
  }

  private JavaRDD<GenericRecord> fetchRecordsFromDataset(Optional<Integer> numPartitions, Optional<Integer> numFiles,
      Optional<Long> numRecordsToUpdate, Optional<Double> percentageRecordsPerFile) throws IOException {
    log.info("NumPartitions " + numPartitions + ", NumFiles " + numFiles + " numRecordsToUpdate " +
        numRecordsToUpdate + " percentageRecordsPerFile " + percentageRecordsPerFile);
    List<String> partitionPaths = getPartitions(numPartitions);
    // Read all file slices in the partition
    JavaPairRDD<String, Iterator<FileSlice>> partitionToFileSlice = getPartitionToFileSlice(metaClient,
        partitionPaths);
    // TODO : read record count from metadata
    // Read the records in a single file
    long recordsInSingleFile = Iterators.size(readParquetOrLogFiles(getSingleSliceFromRDD(partitionToFileSlice)));
    int numFilesToUpdate;
    long numRecordsToUpdatePerFile;
    if (!numFiles.isPresent() || numFiles.get() == 0) {
      // If num files are not passed, find the number of files to update based on total records to update and records
      // per file
      numFilesToUpdate = (int) (numRecordsToUpdate.get() / recordsInSingleFile);
      log.info("Files to Update " + numFilesToUpdate);
      numRecordsToUpdatePerFile = recordsInSingleFile;
    } else {
      // If num files is passed, find the number of records per file based on either percentage or total records to
      // update and num files passed
      numFilesToUpdate = numFiles.get();
      numRecordsToUpdatePerFile = percentageRecordsPerFile.isPresent() ? (long) (recordsInSingleFile
          * percentageRecordsPerFile.get()) : numRecordsToUpdate.get() / numFilesToUpdate;
    }
    // Adjust the number of files to read per partition based on the requested partition & file counts
    Map<String, Integer> adjustedPartitionToFileIdCountMap = getFilesToReadPerPartition(partitionToFileSlice,
        getPartitions(numPartitions).size(), numFilesToUpdate);
    JavaRDD<GenericRecord> updates = projectSchema(generateUpdates(adjustedPartitionToFileIdCountMap,
        partitionToFileSlice, numFilesToUpdate, (int) numRecordsToUpdatePerFile));
    if (numRecordsToUpdate.isPresent() && numFiles.isPresent() && numFiles.get() != 0 && numRecordsToUpdate.get()
        != numRecordsToUpdatePerFile * numFiles.get()) {
      updates = updates.union(projectSchema(generateUpdates(adjustedPartitionToFileIdCountMap,
          partitionToFileSlice, numFilesToUpdate, (int) (numRecordsToUpdate.get() - numRecordsToUpdatePerFile * numFiles
              .get()))));
    }
    log.info("Finished generating updates");
    return updates;
  }

  private JavaRDD<GenericRecord> projectSchema(JavaRDD<GenericRecord> updates) {
    // The records read from the hoodie dataset have the hoodie record fields, rewrite the record to eliminate them
    return updates.map(r -> HoodieAvroUtils.rewriteRecordWithNewSchema(r, new Schema.Parser().parse(schemaStr)));
  }

  private JavaRDD<GenericRecord> generateUpdates(Map<String, Integer> adjustedPartitionToFileIdCountMap,
      JavaPairRDD<String, Iterator<FileSlice>> partitionToFileSlice, int numFiles, int numRecordsToReadPerFile) {
    return partitionToFileSlice.map(p -> {
      int maxFilesToRead = adjustedPartitionToFileIdCountMap.get(p._1);
      return Iterators.limit(p._2, maxFilesToRead);
    }).flatMap(p -> p).repartition(numFiles).map(fileSlice -> {
      if (numRecordsToReadPerFile > 0) {
        return Iterators.limit(readParquetOrLogFiles(fileSlice), numRecordsToReadPerFile);
      } else {
        return readParquetOrLogFiles(fileSlice);
      }
    }).flatMap(p -> p).map(i -> (GenericRecord) i);
  }

  private Map<String, Integer> getFilesToReadPerPartition(JavaPairRDD<String, Iterator<FileSlice>>
      partitionToFileSlice, Integer numPartitions, Integer numFiles) {
    int numFilesPerPartition = (int) Math.ceil(numFiles / numPartitions);
    Map<String, Integer> partitionToFileIdCountMap = partitionToFileSlice.mapToPair(p -> new Tuple2<>(p._1, Iterators
        .size(p._2))).collectAsMap();
    long totalExistingFilesCount = partitionToFileIdCountMap.values().stream().reduce((a, b) -> a + b).get();
    Preconditions.checkArgument(totalExistingFilesCount >= numFiles, "Cannot generate updates "
        + "for more files than present in the dataset, file requested " + numFiles + ", files present "
        + totalExistingFilesCount);
    Map<String, Integer> partitionToFileIdCountSortedMap = partitionToFileIdCountMap
        .entrySet()
        .stream()
        .sorted(comparingByValue())
        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
            LinkedHashMap::new));
    // Limit files to be read per partition
    Map<String, Integer> adjustedPartitionToFileIdCountMap = new HashMap<>();
    partitionToFileIdCountSortedMap.entrySet().stream().forEach(e -> {
      if (e.getValue() <= numFilesPerPartition) {
        adjustedPartitionToFileIdCountMap.put(e.getKey(), e.getValue());
      } else {
        adjustedPartitionToFileIdCountMap.put(e.getKey(), numFilesPerPartition);
      }
    });
    return adjustedPartitionToFileIdCountMap;
  }

  private FileSlice getSingleSliceFromRDD(JavaPairRDD<String, Iterator<FileSlice>> partitionToFileSlice) {
    return partitionToFileSlice.map(f -> {
      FileSlice slice = f._2.next();
      FileSlice newSlice = new FileSlice(slice.getFileGroupId(), slice.getBaseInstantTime());
      if (slice.getDataFile().isPresent()) {
        newSlice.setDataFile(slice.getDataFile().get());
      } else {
        slice.getLogFiles().forEach(l -> {
          newSlice.addLogFile(l);
        });
      }
      return newSlice;
    }).take(1).get(0);
  }

  private Iterator<IndexedRecord> readParquetOrLogFiles(FileSlice fileSlice) throws IOException {
    if (fileSlice.getDataFile().isPresent()) {
      Iterator<IndexedRecord> itr =
          new ParquetReaderIterator<IndexedRecord>(AvroParquetReader.<IndexedRecord>builder(new
              Path(fileSlice.getDataFile().get().getPath())).withConf(metaClient.getHadoopConf()).build());
      return itr;
    } else {
      // If there is no data file, fall back to reading log files
      HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(metaClient.getFs(),
          metaClient.getBasePath(),
          fileSlice.getLogFiles().map(l -> l.getPath().getName()).collect(Collectors.toList()),
          new Schema.Parser().parse(schemaStr), metaClient.getActiveTimeline().getCommitsTimeline()
          .filterCompletedInstants().lastInstant().get().getTimestamp(),
          HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES, true, false,
          HoodieMemoryConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE,
          AbstractRealtimeRecordReader.DEFAULT_SPILLABLE_MAP_BASE_PATH);
      // readAvro log files
      Iterable<HoodieRecord<? extends HoodieRecordPayload>> iterable = () -> scanner.iterator();
      Schema schema = new Schema.Parser().parse(schemaStr);
      return StreamSupport.stream(iterable.spliterator(), false)
          .map(e -> {
            try {
              return (IndexedRecord) e.getData().getInsertValue(schema).get();
            } catch (IOException io) {
              throw new UncheckedIOException(io);
            }
          }).iterator();
    }
  }

  @Override
  public JavaRDD<GenericRecord> read(long numRecords) throws IOException {
    return fetchAnyRecordsFromDataset(Optional.of(numRecords));
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, long approxNumRecords) throws IOException {
    return fetchAnyRecordsFromDataset(Optional.of(approxNumRecords), Optional.of(numPartitions));
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, int numFiles, long numRecords) throws IOException {
    return fetchRecordsFromDataset(Optional.of(numPartitions), Optional.of(numFiles), Optional.of(numRecords));
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, int numFiles, double percentageRecordsPerFile)
      throws IOException {
    return fetchPercentageRecordsFromDataset(Optional.of(numPartitions), Optional.of(numFiles),
        Optional.of(percentageRecordsPerFile));
  }
}
