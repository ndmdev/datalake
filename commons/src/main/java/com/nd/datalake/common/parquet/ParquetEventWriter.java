package com.nd.datalake.common.parquet;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nd.datalake.common.utils.DateUtil;

/**
 * The ParquetRecordWriter class
 *
 * @author krishnaprasad
 *
 */
public class ParquetEventWriter<T extends BaseAvroEvent> implements Runnable {

	private static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;

	private static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024;

	private final int blockSize;

	private final int pageSize;

	private final String outputBaseLocation;

	private Configuration hadoopConf = new Configuration();

	private ParquetRecordTranslator parquetRecordTranslator = null;

	private final Map<String, ParquetWriterWithPath> parquetWriterCache = new ConcurrentHashMap<String, ParquetWriterWithPath>();

	private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

	private long nextDayStart;

	private static final int DEFAULT_ROLLING_INTERVAL = 1440;

	private static final String BASE_LOCATION = "/usr/local";

	private final AtomicBoolean fileRollingStatus = new AtomicBoolean(false);

	private final AtomicBoolean dataPresent = new AtomicBoolean(false);

	private final Logger logger = LoggerFactory.getLogger(ParquetEventWriter.class);

	private Schema avroSchema;

	private final FileMetaStore fileMetaStore;

	private String metaLocation = BASE_LOCATION;

	public ParquetEventWriter(final int pageSize, final ParquetRecordTranslator translator, final int rollingInterval,
			final Schema avroSchema, final String outputBaseLocation) throws IOException {
		this(DEFAULT_BLOCK_SIZE, pageSize, translator, rollingInterval, avroSchema, outputBaseLocation,
				new Configuration(), BASE_LOCATION);
	}

	public ParquetEventWriter(final int blockSize, final int pageSize, final ParquetRecordTranslator translator,
			final int rollingInterval, final Schema avroSchema, final String outputBaseLocation,
			final String metaLocation) throws IOException {
		this(blockSize, pageSize, translator, rollingInterval, avroSchema, outputBaseLocation, new Configuration(),
				metaLocation);
	}

	public ParquetEventWriter(final int blockSize, final int pageSize, final ParquetRecordTranslator translator,
			final int rollingInterval, final Schema avroSchema, final String parquetOutputLocation,
			final Configuration hadoopConf, final String metaLocation) throws IOException {
		super();
		this.blockSize = blockSize;
		this.pageSize = pageSize;
		this.outputBaseLocation = (parquetOutputLocation == null || parquetOutputLocation.isEmpty()) ? BASE_LOCATION
				: parquetOutputLocation;
		this.hadoopConf = hadoopConf;
		this.avroSchema = avroSchema;
		translator.setAvroSchema(avroSchema);
		this.parquetRecordTranslator = translator;
		this.metaLocation = metaLocation;
		setNextDayStart();
		fileMetaStore = new FileMetaStore();
		if (rollingInterval != DEFAULT_ROLLING_INTERVAL) {
			this.scheduledExecutor.scheduleAtFixedRate(this, rollingInterval, rollingInterval, TimeUnit.MINUTES);
		}
	}

	public ParquetEventWriter(final Schema avroSchema, final String parquetOutputLocation,
			final ParquetRecordTranslator translator) throws IOException {
		this(DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, translator, DEFAULT_ROLLING_INTERVAL, avroSchema,
				parquetOutputLocation, BASE_LOCATION);
	}

	public void init() throws IOException {
		logger.info("[ParquetRecordWriter|init|Configuration initialized, BaseLocation:{}]", outputBaseLocation);
	}

	private ParquetWriter<GenericRecord> createParquetWriter(final String parquetOutputFile) throws IOException {
		return AvroParquetWriter.<GenericRecord>builder(new Path(parquetOutputFile)).withConf(hadoopConf)
				.withCompressionCodec(CompressionCodecName.SNAPPY).withSchema(avroSchema)
				.withWriteMode(ParquetFileWriter.Mode.CREATE).withPageSize(pageSize).build();
	}

	public synchronized void write(String feed) throws IOException {
		if (System.currentTimeMillis() >= nextDayStart) {
			doDailyJobs();
		}
		if (fileRollingStatus.get()) {
			rollFile();
		}
		GenericRecord record = parquetRecordTranslator.translate(feed);
		writeToParquet(record);
	}

	private void writeToParquet(GenericRecord record) throws IOException {
		Object partition = record.get("partition_column");
		String partitionColumnValue = (String) partition == null ? "" : (String) partition;
		ParquetWriterWithPath parquetWriterWithPath = parquetWriterCache.get(partitionColumnValue);
		if (parquetWriterWithPath == null) {
			final String path = createParquetFilePath(partitionColumnValue);
			try {
				ParquetWriter<GenericRecord> parquetWriter = createParquetWriter(path);
				parquetWriterWithPath = new ParquetWriterWithPath(parquetWriter, path);
				parquetWriterCache.put(partitionColumnValue, parquetWriterWithPath);
			} catch (final IOException e) {
				logger.error("[ParquetRecordWriter|write|Parquet File Creation Failed for path:" + path + "]", e);
				throw e;
			}
		}
		parquetWriterWithPath.getParquetWriter().write(record);
		dataPresent.compareAndSet(false, true);
	}

	private synchronized void rollFile() {
		CleanParquetWriterCache();
		fileRollingStatus.compareAndSet(true, false);
		fileMetaStore.getNextFileNumber();
	}

	private String createParquetFilePath(final String partitionKey) {
		return new StringBuffer(outputBaseLocation).append("/").append(partitionKey.isEmpty() ? "" : partitionKey + "/")
				.append(getDateString()).append("/").append(fileMetaStore.getCurrentFileNumber()).append(".parquet.tmp")
				.toString();
	}

	private String getDateString() {
		return new SimpleDateFormat("yyyyMMdd").format(new Date());
	}

	private synchronized void doDailyJobs() {
		if (System.currentTimeMillis() >= nextDayStart) {
			CleanParquetWriterCache();
			setNextDayStart();
			fileRollingStatus.compareAndSet(true, false);
			fileMetaStore.clean();
			logger.info("[ParquetRecordWriter|doDailyJobs|Daily cleanup complete]");
		}
	}

	private synchronized void CleanParquetWriterCache() {
		for (final ParquetWriterWithPath pwWithPath : parquetWriterCache.values()) {
			final ParquetWriter<GenericRecord> parquetWriter = pwWithPath.getParquetWriter();
			try {
				parquetWriter.close();
				if (!renameTempFile(pwWithPath.getFilePath())) {
					logger.error("[ParquetRecordWriter|CleanParquetWriterCache|File rename failed for, {}]",
							pwWithPath.getFilePath());
				}
			} catch (final IOException e) {
				logger.error(
						"[ParquetRecordWriter|CleanParquetWriterCache|Failed while closing the parquet writer, {}]",
						pwWithPath.getFilePath(), e);
			}
		}
		logger.info("[ParquetRecordWriter|CleanParquetWriterCache|{} Parquet Writers Closed]",
				parquetWriterCache.entrySet().size());
		parquetWriterCache.clear();
		dataPresent.compareAndSet(true, false);
	}

	private boolean renameTempFile(String filePath) {
		Path src = new Path(filePath);
		int endIndex = filePath.lastIndexOf(".tmp");
		if (endIndex > 0) {
			String dstStr = filePath.substring(0, endIndex);
			Path dst = new Path(dstStr);
			FileSystem fileSystem = null;
			try {
				fileSystem = src.getFileSystem(hadoopConf);
				return fileSystem.rename(src, dst);
			} catch (IOException e) {
				logger.error("[ParquetRecordWriter|renameTempFile|File rename failed for, {}]", filePath, e);
				throw new RuntimeException("File rename failed", e);
			}
		} else {
			logger.error("[ParquetRecordWriter|renameTempFile|File doesn't contain .tmp extension, {}]", filePath);
			return false;
		}

	}

	private synchronized void setNextDayStart() {
		nextDayStart = DateUtil.getDayStart(1);
	}

	public synchronized void destroy() {
		CleanParquetWriterCache();
		fileMetaStore.close();
	}

	@Override
	public void run() {
		if (!fileRollingStatus.compareAndSet(false, true)) {
			if (dataPresent.get()) {
				rollFile();
			} else {
				fileRollingStatus.getAndSet(false);
				logger.info("[ParquetRecordWriter|RollingThread$run|No data to roll]",
						parquetWriterCache.entrySet().size());
			}
		}
	}

	private class FileMetaStore {

		private static final int INDEX = 0;

		private RandomAccessFile randomAccessFile;

		private FileChannel fileChannel;

		private MappedByteBuffer mapBuffer;

		private int currentFileNumber = 0;

		private FileMetaStore() {
			try {
				init();
			} catch (final IOException e) {
				logger.error("[ParquetRecordWriter|FileMetaStore|Meta File(.pw_meta) creation Failed location:"
						+ metaLocation + "]", e);
				throw new RuntimeException(e);
			}
		}

		public void clean() {
			updateFileNumber(1);
		}

		private void init() throws IOException {
			final File file = new File(metaLocation, ".pw_meta");
			if (!file.exists()) {
				file.createNewFile();
			}
			randomAccessFile = new RandomAccessFile(file, "rw");
			fileChannel = randomAccessFile.getChannel();
			mapBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4);
			updateFileNumber(getLastFileNumber() + 1);
		}

		public int getCurrentFileNumber() {
			return currentFileNumber;
		}

		private int getNextFileNumber() {
			updateFileNumber(currentFileNumber + 1);
			return currentFileNumber;
		}

		private int getLastFileNumber() {
			return mapBuffer.getInt(INDEX);
		}

		private void updateFileNumber(final int number) {
			mapBuffer.putInt(INDEX, number);
			currentFileNumber = number;
		}

		private void close() {
			try {
				if (fileChannel != null) {
					fileChannel.close();
				}
				if (randomAccessFile != null) {
					randomAccessFile.close();
				}
			} catch (final IOException e) {
				logger.error("[ParquetRecordWriter|FileMetaStore|close|Meta File(.pw_meta) Close Operation Failed]", e);
			}

		}
	}

	private class ParquetWriterWithPath {

		private final ParquetWriter<GenericRecord> parquetWriter;

		private final String filePath;

		public ParquetWriterWithPath(ParquetWriter<GenericRecord> parquetWriter, String filePath) {
			super();
			this.parquetWriter = parquetWriter;
			this.filePath = filePath;
		}

		public ParquetWriter<GenericRecord> getParquetWriter() {
			return parquetWriter;
		}

		public String getFilePath() {
			return filePath;
		}

	}

}
