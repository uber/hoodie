/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.InvalidHoodiePathException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Utility functions related to accessing the file storage
 */
public class FSUtils {

    private static final Logger LOG = LogManager.getLogger(FSUtils.class);
    // Log files are of this pattern - b5068208-e1a4-11e6-bf01-fe55135034f3_20170101134598.avro.delta.1
    private static final Pattern LOG_FILE_PATTERN = Pattern.compile("(.*)_(.*)\\.(.*)\\.(.*)\\.([0-9]*)");
    private static final int MAX_ATTEMPTS_RECOVER_LEASE = 10;
    private static final long MIN_CLEAN_TO_KEEP = 10;
    private static final long MIN_ROLLBACK_TO_KEEP = 10;
    private static FileSystem fs;

    /**
     * Only to be used for testing.
     */
    @VisibleForTesting
    public static void setFs(FileSystem fs) {
        FSUtils.fs = fs;
    }


    public static FileSystem getFs() {
        if (fs != null) {
            return fs;
        }
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new HoodieIOException("Failed to get instance of " + FileSystem.class.getName(),
                e);
        }
        LOG.info(String.format("Hadoop Configuration: fs.defaultFS: [%s], Config:[%s], FileSystem: [%s]",
                conf.getRaw("fs.defaultFS"), conf.toString(), fs.toString()));

        return fs;
    }

    public static String makeDataFileName(String commitTime, int taskPartitionId, String fileId) {
        return String.format("%s_%d_%s.parquet", fileId, taskPartitionId, commitTime);
    }

    public static String maskWithoutFileId(String commitTime, int taskPartitionId) {
        return String.format("*_%s_%s.parquet", taskPartitionId, commitTime);
    }

    public static String maskWithoutTaskPartitionId(String commitTime, String fileId) {
        return String.format("%s_*_%s.parquet", fileId, commitTime);
    }

    public static String maskWithOnlyCommitTime(String commitTime) {
        return String.format("*_*_%s.parquet", commitTime);
    }

    public static String getCommitFromCommitFile(String commitFileName) {
        return commitFileName.split("\\.")[0];
    }

    public static String getCommitTime(String fullFileName) {
        return fullFileName.split("_")[2].split("\\.")[0];
    }

    public static long getFileSize(FileSystem fs, Path path) throws IOException {
        return fs.listStatus(path)[0].getLen();
    }

    public static String globAllFiles(String basePath) {
        return String.format("%s/*/*/*/*", basePath);
    }

    // TODO (weiy): rename the function for better readability
    public static String getFileId(String fullFileName) {
        return fullFileName.split("_")[0];
    }

    /**
     * Obtain all the partition paths, that are present in this table.
     */
    public static List<String> getAllPartitionPaths(FileSystem fs, String basePath)
        throws IOException {
        List<String> partitionsToClean = new ArrayList<>();
        // TODO(vc): For now, assume partitions are two levels down from base path.
        FileStatus[] folders = fs.globStatus(new Path(basePath + "/*/*/*"));
        for (FileStatus status : folders) {
            Path path = status.getPath();
            partitionsToClean.add(String.format("%s/%s/%s", path.getParent().getParent().getName(),
                path.getParent().getName(), path.getName()));
        }
        return partitionsToClean;
    }

    public static String getFileExtension(String fullName) {
        Preconditions.checkNotNull(fullName);
        String fileName = (new File(fullName)).getName();
        int dotIndex = fileName.indexOf('.');
        return dotIndex == -1 ? "" : fileName.substring(dotIndex);
    }

    public static String getInstantTime(String name) {
        return name.replace(getFileExtension(name), "");
    }


    /**
     * Get the file extension from the log file
     * @param logPath
     * @return
     */
    public static String getFileExtensionFromLog(Path logPath) {
        Matcher matcher = LOG_FILE_PATTERN.matcher(logPath.getName());
        if(!matcher.find()) {
            throw new InvalidHoodiePathException(logPath, "LogFile");
        }
        return matcher.group(3) + "." + matcher.group(4);
    }

    /**
     * Get the first part of the file name in the log file. That will be the fileId.
     * Log file do not have commitTime in the file name.
     *
     * @param path
     * @return
     */
    public static String getFileIdFromLogPath(Path path) {
        Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
        if(!matcher.find()) {
            throw new InvalidHoodiePathException(path, "LogFile");
        }
        return matcher.group(1);
    }

    /**
     * Get the first part of the file name in the log file. That will be the fileId.
     * Log file do not have commitTime in the file name.
     *
     * @param path
     * @return
     */
    public static String getBaseCommitTimeFromLogPath(Path path) {
        Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
        if(!matcher.find()) {
            throw new InvalidHoodiePathException(path, "LogFile");
        }
        return matcher.group(2);
    }

    /**
     * Get the last part of the file name in the log file and convert to int.
     *
     * @param logPath
     * @return
     */
    public static int getFileVersionFromLog(Path logPath) {
        Matcher matcher = LOG_FILE_PATTERN.matcher(logPath.getName());
        if(!matcher.find()) {
            throw new InvalidHoodiePathException(logPath, "LogFile");
        }
        return Integer.parseInt(matcher.group(5));
    }

    public static String makeLogFileName(String fileId, String logFileExtension,
        String baseCommitTime, int version) {
        return String.format("%s_%s%s.%d", fileId, baseCommitTime, logFileExtension, version);
    }

    public static String maskWithoutLogVersion(String commitTime, String fileId, String logFileExtension) {
        return String.format("%s_%s%s*", fileId, commitTime, logFileExtension);
    }


    /**
     * Get the latest log file written from the list of log files passed in
     *
     * @param logFiles
     * @return
     */
    public static Optional<HoodieLogFile> getLatestLogFile(Stream<HoodieLogFile> logFiles) {
        return logFiles.sorted(Comparator
            .comparing(s -> s.getLogVersion(),
                Comparator.reverseOrder())).findFirst();
    }

    /**
     * Get all the log files for the passed in FileId in the partition path
     *
     * @param fs
     * @param partitionPath
     * @param fileId
     * @param logFileExtension
     * @return
     */
    public static Stream<HoodieLogFile> getAllLogFiles(FileSystem fs, Path partitionPath,
        final String fileId, final String logFileExtension, final String baseCommitTime) throws IOException {
        return Arrays.stream(fs.listStatus(partitionPath,
            path -> path.getName().startsWith(fileId) && path.getName().contains(logFileExtension)))
            .map(HoodieLogFile::new).filter(s -> s.getBaseCommitTime().equals(baseCommitTime));
    }

    /**
     * Get the latest log version for the fileId in the partition path
     *
     * @param fs
     * @param partitionPath
     * @param fileId
     * @param logFileExtension
     * @return
     * @throws IOException
     */
    public static Optional<Integer> getLatestLogVersion(FileSystem fs, Path partitionPath,
        final String fileId, final String logFileExtension, final String baseCommitTime) throws IOException {
        Optional<HoodieLogFile> latestLogFile =
            getLatestLogFile(getAllLogFiles(fs, partitionPath, fileId, logFileExtension, baseCommitTime));
        if (latestLogFile.isPresent()) {
            return Optional.of(latestLogFile.get().getLogVersion());
        }
        return Optional.empty();
    }

    public static int getCurrentLogVersion(FileSystem fs, Path partitionPath,
        final String fileId, final String logFileExtension, final String baseCommitTime) throws IOException {
        Optional<Integer> currentVersion =
            getLatestLogVersion(fs, partitionPath, fileId, logFileExtension, baseCommitTime);
        // handle potential overflow
        return (currentVersion.isPresent()) ? currentVersion.get() : 1;
    }

    /**
     * computes the next log version for the specified fileId in the partition path
     *
     * @param fs
     * @param partitionPath
     * @param fileId
     * @return
     * @throws IOException
     */
    public static int computeNextLogVersion(FileSystem fs, Path partitionPath, final String fileId,
        final String logFileExtension, final String baseCommitTime) throws IOException {
        Optional<Integer> currentVersion =
            getLatestLogVersion(fs, partitionPath, fileId, logFileExtension, baseCommitTime);
        // handle potential overflow
        return (currentVersion.isPresent()) ? currentVersion.get() + 1 : 1;
    }

    public static int getDefaultBufferSize(final FileSystem fs) {
        return fs.getConf().getInt("io.file.buffer.size", 4096);
    }

    public static Short getDefaultReplication(FileSystem fs, Path path) {
        return fs.getDefaultReplication(path);
    }

    public static Long getDefaultBlockSize(FileSystem fs, Path path) {
        return fs.getDefaultBlockSize(path);
    }

    /**
     * When a file was opened and the task died without closing the stream, another task executor cannot open because the existing lease will be active.
     * We will try to recover the lease, from HDFS. If a data node went down, it takes about 10 minutes for the lease to be rocovered.
     * But if the client dies, this should be instant.
     *
     * @param dfs
     * @param p
     * @return
     * @throws IOException
     */
    public static boolean recoverDFSFileLease(final DistributedFileSystem dfs, final Path p)
        throws IOException, InterruptedException {
        LOG.info("Recover lease on dfs file " + p);
        // initiate the recovery
        boolean recovered = false;
        for (int nbAttempt = 0; nbAttempt < MAX_ATTEMPTS_RECOVER_LEASE; nbAttempt++) {
            LOG.info("Attempt " + nbAttempt + " to recover lease on dfs file " + p);
            recovered = dfs.recoverLease(p);
            if (recovered)
                break;
            // Sleep for 1 second before trying again. Typically it takes about 2-3 seconds to recover under default settings
            Thread.sleep(1000);
        }
        return recovered;

    }

    public static void deleteOlderCleanMetaFiles(FileSystem fs, String metaPath,
        Stream<HoodieInstant> instants) {
        //TODO - this should be archived when archival is made general for all meta-data
        // skip MIN_CLEAN_TO_KEEP and delete rest
        instants.skip(MIN_CLEAN_TO_KEEP).map(s -> {
            try {
                return fs.delete(new Path(metaPath, s.getFileName()), false);
            } catch (IOException e) {
                throw new HoodieIOException("Could not delete clean meta files" + s.getFileName(),
                    e);
            }
        });
    }

    public static void deleteOlderRollbackMetaFiles(FileSystem fs, String metaPath,
        Stream<HoodieInstant> instants) {
        //TODO - this should be archived when archival is made general for all meta-data
        // skip MIN_ROLLBACK_TO_KEEP and delete rest
        instants.skip(MIN_ROLLBACK_TO_KEEP).map(s -> {
            try {
                return fs.delete(new Path(metaPath, s.getFileName()), false);
            } catch (IOException e) {
                throw new HoodieIOException(
                    "Could not delete rollback meta files " + s.getFileName(), e);
            }
        });
    }

    public static void createPathIfNotExists(FileSystem fs, Path partitionPath) throws IOException {
        if(!fs.exists(partitionPath)) {
            fs.mkdirs(partitionPath);
        }
    }
}
