package sg.adsc.jkadbear.flink;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * This SourceFunction geneservingSpeeds a data stream of String records which are
 * read from a gzipped input file. Each record has a time stamp and the input file must be
 * ordered by this time stamp.
 *
 * In order to simulate a realistic stream source, the SourceFunction serves events proportional to
 * their timestamps. In addition, the serving of events can be delayed by a bounded random delay
 * which causes the events to be served slightly out-of-order of their timestamps.
 *
 * The serving speed of the SourceFunction can be adjusted by a serving speed factor.
 * A factor of 60.0 increases the logical serving time by a factor of 60, i.e., events of one
 * minute (60 seconds) are served in 1 second.
 *
 * This SourceFunction is an EventSourceFunction and does continuously emit watermarks.
 * Hence it can only opeservingSpeed in event time mode which is configured as follows:
 *
 *   StreamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 *
 */
public class TweetSource implements SourceFunction<String> {

	private static final Logger LOG = LoggerFactory.getLogger(TweetSource.class);

	private final int watermarkDelayMSecs;

	private final String dataFilePath;
	private final int servingSpeed;
	private final int batchTimeInteval;
	private final int batchPerSec;
	private final int watermarkerInterval;
	private final int wmCntPerSecond;

	private transient BufferedReader reader;
	private boolean isRunning;
	private boolean isS3;

	public TweetSource(String dataFilePath, int servingSpeedFactor) {
		this(dataFilePath, servingSpeedFactor, false);
	}

	public TweetSource(String dataFilePath, int servingSpeedFactor, boolean isS3) {
		this.batchPerSec = 10;
		if (servingSpeedFactor / this.batchPerSec < 1) {
			throw new IllegalArgumentException(String.format("rate must be larger than %d", this.batchPerSec));
		}
		this.dataFilePath = dataFilePath;
		this.watermarkDelayMSecs = 10000;
		this.batchTimeInteval = 1000 / this.batchPerSec; // ms
		this.watermarkerInterval = 100; // ms
		this.wmCntPerSecond = 1000 / this.watermarkerInterval;
		this.servingSpeed = servingSpeedFactor / (1000 / this.batchTimeInteval);
		this.isRunning = true;
		this.isS3 = isS3;
	}

	@Override
	public void run(SourceContext<String> sourceContext) throws Exception {

		generateSpeedOrderedStream(sourceContext);

		this.reader.close();
		this.reader = null;
	}

	private void generateSpeedOrderedStream(SourceContext<String> sourceContext) throws Exception {
		long totalSize = 0;
		long totalSentEle = 0;
		int emptyCnt = -1; // because first line of the source file is empty, set -1 to ensure the first batch size
		long markerCnt = 0;

		long markerId = -1;
		long startTime = System.currentTimeMillis();
		long wakeupTime = System.currentTimeMillis();

		class WaterMarkerGenerator extends Thread {
			private Long markerId;
			private Long markerCnt;

			private WaterMarkerGenerator() {
				this.markerId = -1L;
				this.markerCnt = 0L;
			}

			@Override
			public void run() {
				while (isRunning) {
					long now = System.currentTimeMillis();
//                    sourceContext.emitWatermark(new Watermark(now));
					sourceContext.emitWatermark(new Watermark(now - (now % watermarkerInterval)));
					markerCnt++;
					if (markerCnt % wmCntPerSecond == 0) {
						LOG.info(String.format("#*FROMSOURCE\t%d %d", markerId, System.currentTimeMillis()));
//                        System.out.println(String.format("#*FROMSOURCE\t%d %d", markerId, now));
						String markerStr = String.format("%032d%032d", markerId--, now - (now % watermarkerInterval));
						sourceContext.collectWithTimestamp(markerStr, now - (now % watermarkerInterval));
					}
					try {
						Thread.sleep(watermarkerInterval);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}

		// generate watermarker
//        new WaterMarkerGenerator().start();

		boolean first = true;
		while (isRunning) {
			try {
				if (isS3) {
					AmazonS3 s3 = new AmazonS3Client(new ProfileCredentialsProvider());
					s3.setRegion(Region.getRegion(Regions.EU_WEST_1));
					String bucket = dataFilePath.split("/")[2];
					String endpoint = dataFilePath.split("/")[3];
					S3Object s3object = s3.getObject(new GetObjectRequest(bucket, endpoint));
					reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
				}
				else {
					reader = new BufferedReader(new FileReader(new File(dataFilePath)));
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			try {
				if (first) {
					startTime = System.currentTimeMillis();
					wakeupTime = startTime;
					first = false;
				}
				String line;
				while ((line = reader.readLine()) != null && isRunning) {
					if (emptyCnt == servingSpeed) {
						markerCnt++;
						// emit 1 BatchMarker
						if (markerCnt % 10 == 0) {
							long now = System.currentTimeMillis();
							LOG.info(String.format("#*FROMSOURCE\t%d %d", markerId, now));
							String markerStr = String.format("%032d%032d", markerId--, now);
							sourceContext.collectWithTimestamp(markerStr, now);
						}

						long cTime = System.currentTimeMillis();
						long sleepTime = batchTimeInteval - (cTime - wakeupTime);
						if (sleepTime > 0) {
							Thread.sleep(sleepTime);
//                            LOG.info(String.format("sleeptime: %dms (cTime: %dms, wTime: %dms)", sleepTime, cTime, wakeupTime));
						} else {
							LOG.info(String.format("markerId: %d, negative sleeptime: %dms (cTime: %dms, wTime: %dms), no enough time to emit a batch",
									markerId, sleepTime, cTime, wakeupTime));
							LOG.info(String.format("Throughput: %.2f MB/s", totalSize / 1024 / 1024 / ((cTime - startTime)/1000.0)));
						}

//                        long now = System.currentTimeMillis();
//                        sourceContext.emitWatermark(new Watermark(now - watermarkerInterval));

						emptyCnt = 0;
						wakeupTime += batchTimeInteval;
					}
					if (line.equals(" ")) {
						emptyCnt++;
						continue;
					}
					totalSentEle++;
					totalSize += (double) line.getBytes().length;
					Long timeStamp = System.currentTimeMillis();
					sourceContext.collectWithTimestamp(String.format("%032d%032d%s", 1, timeStamp, line), timeStamp);
				}
				reader.close();
				reader = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void cancel() {
		try {
			if (this.reader != null) {
				this.reader.close();
			}
		} catch(IOException ioe) {
			throw new RuntimeException("Could not cancel SourceFunction", ioe);
		} finally {
			this.reader = null;
			isRunning = false;
		}
	}

}
