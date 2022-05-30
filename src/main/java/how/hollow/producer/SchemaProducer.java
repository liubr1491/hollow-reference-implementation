package how.hollow.producer;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemAnnouncementWatcher;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemBlobRetriever;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.schema.HollowSchema;
import com.netflix.hollow.core.schema.HollowSchemaParser;
import com.netflix.hollow.core.write.HollowObjectWriteRecord;
import how.hollow.DataModelSchema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class SchemaProducer {

    public static final String SCRATCH_DIR = System.getProperty("java.io.tmpdir");
    private static final long MIN_TIME_BETWEEN_CYCLES = 10000;

    public static void main(String[] args) throws IOException {

        File publishDir = new File(SCRATCH_DIR, "publish-dir");
        publishDir.mkdir();
        Path publishPath = Paths.get(publishDir.toURI());

        System.out.println("I AM THE PRODUCER.  I WILL PUBLISH TO " + publishDir.getAbsolutePath());

        HollowProducer.Publisher publisher = new HollowFilesystemPublisher(publishPath);
        HollowProducer.Announcer announcer = new HollowFilesystemAnnouncer(publishPath);

        HollowConsumer.BlobRetriever blobRetriever = new HollowFilesystemBlobRetriever(publishPath);
        HollowConsumer.AnnouncementWatcher announcementWatcher = new HollowFilesystemAnnouncementWatcher(publishPath);

        HollowProducer producer = HollowProducer.withPublisher(publisher)
                .withAnnouncer(announcer)
                .build();

        List<HollowSchema> schemas = HollowSchemaParser.parseCollectionOfSchemas(DataModelSchema.ALL_SCHEMA);
        for (HollowSchema schema : schemas) {
            producer.initializeDataModel(schema);
        }

        restoreIfAvailable(producer, blobRetriever, announcementWatcher);

        cycleForever(producer);

    }

    public static void restoreIfAvailable(HollowProducer producer,
                                          HollowConsumer.BlobRetriever retriever,
                                          HollowConsumer.AnnouncementWatcher unpinnableAnnouncementWatcher) {

        System.out.println("ATTEMPTING TO RESTORE PRIOR STATE...");
        long latestVersion = unpinnableAnnouncementWatcher.getLatestVersion();
        if (latestVersion != HollowConsumer.AnnouncementWatcher.NO_ANNOUNCEMENT_AVAILABLE) {
            producer.restore(latestVersion, retriever);
            System.out.println("RESTORED " + latestVersion);
        } else {
            System.out.println("RESTORE NOT AVAILABLE");
        }
    }

    public static void cycleForever(HollowProducer producer) throws IOException {

        long lastCycleTime = Long.MIN_VALUE;
        while (true) {
            waitForMinCycleTime(lastCycleTime);
            lastCycleTime = System.currentTimeMillis();

            producer.runCycle(writeState -> {
                for (HollowSchema schema : producer.getObjectMapper().getStateEngine().getSchemas()) {
                    switch (schema.getSchemaType()) {
                        case OBJECT: {
                            HollowObjectSchema objectSchema = (HollowObjectSchema) schema;
                            HollowObjectWriteRecord objectWriteRecord = new HollowObjectWriteRecord(objectSchema);
                            for (int i = 0; i < 1000; i++) {
                                objectWriteRecord.reset();
                                for (int i1 = 0; i1 < objectSchema.numFields(); i1++) {
                                    switch (objectSchema.getFieldType(i1)) {
                                        case INT:
                                            objectWriteRecord.setInt(objectSchema.getFieldName(i1), i);
                                            break;
                                        case LONG:
                                            objectWriteRecord.setLong(objectSchema.getFieldName(i1), i);
                                            break;
                                        case STRING:
                                            objectWriteRecord.setString(objectSchema.getFieldName(i1), String.format("test%d-%d", i, System.currentTimeMillis()));
                                            break;
                                        default:
                                            break;
                                    }
                                }
                                writeState.getStateEngine().add(schema.getName(), objectWriteRecord);
                            }
                        }
                        break;
                        case SET:
                        case MAP:
                        case LIST:
                        default:
                            break;
                    }

                }
            });
        }
    }

    private static void waitForMinCycleTime(long lastCycleTime) {
        long targetNextCycleTime = lastCycleTime + MIN_TIME_BETWEEN_CYCLES;

        while (System.currentTimeMillis() < targetNextCycleTime) {
            try {
                Thread.sleep(targetNextCycleTime - System.currentTimeMillis());
            } catch (InterruptedException ignore) {
            }
        }
    }
}
