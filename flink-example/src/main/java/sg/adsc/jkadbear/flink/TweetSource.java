/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sg.adsc.jkadbear.flink;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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

	private transient BufferedReader reader;
    private boolean isRunning;

	/**
	 * Serves the String records from the specified and ordered gzipped input file.
	 * Rides are served exactly in order of their time stamps
	 * at the speed at which they were originally geneservingSpeedd.
	 *
	 * @param dataFilePath The gzipped input file from which the String records are read.
	 */
	public TweetSource(String dataFilePath) {
		this(dataFilePath, 1);
	}

	/**
	 * Serves the String records from the specified and ordered gzipped input file.
	 * Rides are served exactly in order of their time stamps
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the String records are read.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public TweetSource(String dataFilePath, int servingSpeedFactor) {
        this.dataFilePath = dataFilePath;
        this.watermarkDelayMSecs = 10000;
        this.servingSpeed = servingSpeedFactor;
        this.isRunning = true;
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

        long markerId = -1;
        long wakeupTime;
        long batchTimeInteval = 1000; // 1000 ms

		while (isRunning) {
            try {
                if (false) {
                    AmazonS3 s3 = new AmazonS3Client(new ProfileCredentialsProvider());
                    s3.setRegion(Region.getRegion(Regions.EU_WEST_1));
                    S3Object s3object = s3.getObject(new GetObjectRequest("flink-cluster", "splittweetstream.txt"));
                    reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
                }
                else {
                    reader = new BufferedReader(new FileReader(new File(dataFilePath)));
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

			try {
                wakeupTime = System.currentTimeMillis();
				String line;
				while ((line = reader.readLine()) != null && isRunning) {
					if (emptyCnt == servingSpeed) {
						// emit BatchMarker
						String markerStr = String.format("%032d%032d%032d", markerId--, System.currentTimeMillis(), wakeupTime);
//                        LOG.info("markerStr: " + markerStr);

						// send 3 markers
						for (int i = 0; i < 3; i++) {
							sourceContext.collect(markerStr);
						}
//                            LOG.info(String.format("During the last %d ms, we sent %d elements. Sending Rate: %.2f tuples/sec, %.3f MB/sec.",
//                                    System.currentTimeMillis() - wakeupTime, totalSentEle,
//                                    totalSentEle * batchTimeInteval / 1000.0,
//                                    totalSize * batchTimeInteval / 1000.0 / 1024 / 1024));
                        long sleepTime = batchTimeInteval - (System.currentTimeMillis() - wakeupTime);
                        if (sleepTime > 0) {
//                            LOG.info("SleepTime: " + sleepTime + "ms");
                            Thread.sleep(sleepTime);
                        } else {
                            LOG.warn(String.format("negative sleeptime: %dms, no enough time to emit a batch", sleepTime));
                        }
//                        sourceContext.emitWatermark(new Watermark(System.currentTimeMillis()));
						emptyCnt = 0;
						totalSize = 0;
						totalSentEle = 0;
                        wakeupTime = System.currentTimeMillis();
					}
                    if (line.equals(" ")) {
                        emptyCnt++;
                        continue;
                    }
                    totalSentEle++;
                    totalSize += (double) line.getBytes().length;
                    sourceContext.collect(String.format("%032d%032d%s", -markerId, System.currentTimeMillis(), line));
				}
				reader.close();
				reader = null;
			} catch (IOException | InterruptedException e) {
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

