package how.hollow.consumer;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemAnnouncementWatcher;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemBlobRetriever;
import com.netflix.hollow.api.objects.generic.GenericHollowObject;
import com.netflix.hollow.api.sampling.SampleResult;
import com.netflix.hollow.core.schema.HollowSchema;
import com.netflix.hollow.explorer.ui.jetty.HollowExplorerUIServer;
import com.netflix.hollow.history.ui.jetty.HollowHistoryUIServer;
import how.hollow.consumer.api.generated.*;
import how.hollow.producer.SchemaProducer;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.BitSet;

public class SchemaConsumer {

    public static void main(String args[]) throws Exception {
        File publishDir = new File(SchemaProducer.SCRATCH_DIR, "publish-dir");
        Path publishPath = Paths.get(publishDir.toURI());

        System.out.println("I AM THE CONSUMER.  I WILL READ FROM " + publishDir.getAbsolutePath());

        HollowConsumer.BlobRetriever blobRetriever = new HollowFilesystemBlobRetriever(publishPath);
        HollowConsumer.AnnouncementWatcher announcementWatcher = new HollowFilesystemAnnouncementWatcher(publishPath);

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobRetriever)
                .withAnnouncementWatcher(announcementWatcher)
//                .withGeneratedAPIClass(MovieAPI.class)
                .build();

        consumer.triggerRefresh();

        hereIsHowToUseTheDataProgrammatically(consumer);

        /// start a history server on port 7777
        HollowHistoryUIServer historyServer = new HollowHistoryUIServer(consumer, 7777);
        historyServer.start();

        /// start an explorer server on port 7778
        HollowExplorerUIServer explorerServer = new HollowExplorerUIServer(consumer, 7778);
        explorerServer.start();

        historyServer.join();
    }

    private static void hereIsHowToUseTheDataProgrammatically(HollowConsumer consumer) {
//        MovieAPI movieApi = (MovieAPI) consumer.getAPI();
//        for (SampleResult boxedSampleResult : movieApi.getBoxedSampleResults()) {
//            System.out.println(boxedSampleResult.getIdentifier());
//        }
//
//        for (Movie movie : movieApi.getAllMovie()) {
//            System.out.println(movie.getId() + ", " + movie.getTitle().getValue());
//        }

        for (HollowSchema hollowSchema : consumer.getStateEngine().getSchemas()) {
            if (!HollowSchema.SchemaType.OBJECT.equals(hollowSchema.getSchemaType())) {
                continue;
            }

            BitSet populatedOrdinals = consumer.getStateEngine().getTypeState(hollowSchema.getName()).getPopulatedOrdinals();

            int ordinal = populatedOrdinals.nextSetBit(0);
            while (ordinal != -1) {
                GenericHollowObject hollowObject = new GenericHollowObject(
                        consumer.getAPI().getDataAccess(), hollowSchema.getName(), ordinal);

                System.out.println(hollowObject.getLong("id") + ", " + hollowObject.getString("title"));

                ordinal = populatedOrdinals.nextSetBit(ordinal + 1);
            }
        }
    }
}
