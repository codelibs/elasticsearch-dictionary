package org.codelibs.elasticsearch.dictionary.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.codelibs.elasticsearch.dictionary.DictionaryConstants;
import org.codelibs.elasticsearch.dictionary.DictionaryException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBuffers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.snapshots.SnapshotsService.CreateSnapshotListener;
import org.elasticsearch.snapshots.SnapshotsService.SnapshotCompletionListener;
import org.elasticsearch.snapshots.SnapshotsService.SnapshotRequest;

public class DictionarySnapshotService extends AbstractComponent implements
        SnapshotCompletionListener {

    private SnapshotsService snapshotsService;

    private IndicesService indicesService;

    private Environment env;

    private String[] fileExtensions;

    private String dictionaryIndex;

    private Client client;

    private TimeValue masterNodeTimeout;

    @Inject
    public DictionarySnapshotService(final Settings settings,
            final Client client, final Environment env,
            final IndicesService indicesService,
            final SnapshotsService snapshotsService) {
        super(settings);
        this.client = client;
        this.env = env;
        this.indicesService = indicesService;
        this.snapshotsService = snapshotsService;

        dictionaryIndex = settings.get("dictionary.index", ".dictionary");
        fileExtensions = settings.getAsArray("directory.file_extensions",
                new String[] { "txt" });
        masterNodeTimeout = settings.getAsTime(
                "dictionary.snapshot.master_node_timeout",
                TimeValue.timeValueSeconds(30));

        snapshotsService.addListener(this);
    }

    @Override
    public void onSnapshotCompletion(final SnapshotId snapshotId,
            final SnapshotInfo snapshot) {
        if (snapshotId.getSnapshot().contains(dictionaryIndex)) {
            return;
        }

        // find dictionary files
        final Map<String, List<Tuple<String, File>>> indexDictionaryMap = new HashMap<>();
        for (final String index : snapshot.indices()) {
            final IndexService indexService = indicesService
                    .indexService(index);
            final IndexSettingsService settingsService = indexService
                    .settingsService();
            final Settings indexSettings = settingsService.getSettings();
            final List<Tuple<String, File>> dictionaryList = new ArrayList<>();
            for (final Map.Entry<String, String> entry : indexSettings
                    .getAsMap().entrySet()) {
                final String value = entry.getValue();
                if (isDictionaryFile(value)) {
                    addDictionaryFile(dictionaryList, value);
                }
            }
            if (!dictionaryList.isEmpty()) {
                indexDictionaryMap.put(index, dictionaryList);
            }
        }

        if (!indexDictionaryMap.isEmpty()) {
            if (logger.isDebugEnabled()) {
                for (final Map.Entry<String, List<Tuple<String, File>>> entry : indexDictionaryMap
                        .entrySet()) {
                    for (final Tuple<String, File> dictInfo : entry.getValue()) {
                        logger.debug("{}/{} is found in {}.", dictInfo.v1(),
                                dictInfo.v2(), entry.getKey());
                    }
                }
            }
            snapshotDictionaryIndex(snapshotId, indexDictionaryMap);
        }
    }

    private void snapshotDictionaryIndex(final SnapshotId snapshotId,
            final Map<String, List<Tuple<String, File>>> indexDictionaryMap) {
        final String index = dictionaryIndex + "_" + snapshotId.getRepository()
                + "_" + snapshotId.getSnapshot();
        if (logger.isDebugEnabled()) {
            logger.debug("Creating dictionary index: {}", index);
        }
        final CreateIndexRequestBuilder builder = client.admin().indices()
                .prepareCreate(index);
        try {
            for (final String type : indexDictionaryMap.keySet()) {
                builder.addMapping(type, createDictionaryMappingBuilder(type));
            }

            builder.execute(new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(final CreateIndexResponse response) {
                    if (!response.isAcknowledged()) {
                        throw new DictionaryException("Failed to create "
                                + index);
                    }

                    try {
                        writeDictionaryIndex(index, snapshotId,
                                indexDictionaryMap);
                    } catch (final Exception e) {
                        deleteDictionaryIndex(index);
                        throw e;
                    }
                }

                @Override
                public void onFailure(final Throwable e) {
                    logger.error("Failed to take a snapshot for {}.", e,
                            dictionaryIndex);
                }
            });
        } catch (final Exception e) {
            logger.error("Failed to take a snapshot for {}.", e,
                    dictionaryIndex);
        }
    }

    private void writeDictionaryIndex(final String index,
            final SnapshotId snapshotId,
            final Map<String, List<Tuple<String, File>>> indexDictionaryMap) {
        final Queue<Tuple<String, Tuple<String, File>>> dictionaryQueue = new LinkedList<>();
        for (final Map.Entry<String, List<Tuple<String, File>>> entry : indexDictionaryMap
                .entrySet()) {
            final String type = entry.getKey();
            final List<Tuple<String, File>> dictFileList = entry.getValue();
            for (final Tuple<String, File> dictionaryInfo : dictFileList) {
                final Tuple<String, Tuple<String, File>> dictionarySet = new Tuple<>(
                        type, dictionaryInfo);
                dictionaryQueue.add(dictionarySet);
            }
        }

        writeDictionaryFile(index, dictionaryQueue, new ActionListener<Void>() {

            @Override
            public void onResponse(final Void response) {
                flushDictionaryIndex(index, snapshotId);
            }

            @Override
            public void onFailure(final Throwable e) {
                deleteDictionaryIndex(index);
                logger.error("Failed to write data to {}.", e, index);
            }
        });
    }

    private void flushDictionaryIndex(final String index,
            final SnapshotId snapshotId) {
        client.admin().indices().prepareFlush(index).setWaitIfOngoing(true)
                .execute(new ActionListener<FlushResponse>() {

                    @Override
                    public void onResponse(final FlushResponse response) {
                        client.admin()
                                .cluster()
                                .prepareHealth(index)
                                .setWaitForGreenStatus()
                                .execute(
                                        new ActionListener<ClusterHealthResponse>() {

                                            @Override
                                            public void onResponse(
                                                    final ClusterHealthResponse response) {
                                                createDictionarySnapshot(index,
                                                        snapshotId);
                                            }

                                            @Override
                                            public void onFailure(
                                                    final Throwable e) {
                                                deleteDictionaryIndex(index);
                                                logger.error(
                                                        "Failed to flush data to {}.",
                                                        e, index);
                                            }
                                        });
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        deleteDictionaryIndex(index);
                        logger.error("Failed to flush data to {}.", e, index);
                    }

                });
    }

    private void createDictionarySnapshot(final String index,
            final SnapshotId snapshotId) {
        final String repository = snapshotId.getRepository();
        final String snapshot = snapshotId.getSnapshot();
        final String name = snapshot + index;
        final SnapshotRequest request = new SnapshotRequest(
                "create dictionary snapshot", name, repository);
        request.indices(new String[] { index });
        request.masterNodeTimeout(masterNodeTimeout);
        snapshotsService.createSnapshot(request, new CreateSnapshotListener() {
            @Override
            public void onResponse() {
                snapshotsService
                        .addListener(new SnapshotsService.SnapshotCompletionListener() {
                            SnapshotId snapshotId = new SnapshotId(repository,
                                    snapshot);

                            @Override
                            public void onSnapshotCompletion(
                                    final SnapshotId snapshotId2,
                                    final SnapshotInfo snapshot) {
                                if (snapshotId.equals(snapshotId)) {
                                    deleteDictionaryIndex(index);
                                    snapshotsService.removeListener(this);
                                }
                            }

                            @Override
                            public void onSnapshotFailure(
                                    final SnapshotId snapshotId2,
                                    final Throwable t) {
                                if (snapshotId.equals(snapshotId)) {
                                    deleteDictionaryIndex(index);
                                    snapshotsService.removeListener(this);
                                }
                            }
                        });
            }

            @Override
            public void onFailure(final Throwable t) {
                deleteDictionaryIndex(index);
                logger.error("Failed to take a snapshot for {}.", t, index);
            }
        });
    }

    private void deleteDictionaryIndex(final String index) {
        if (logger.isDebugEnabled()) {
            logger.debug("Deleteing {} index.", index);
        }
        client.admin().indices().prepareDelete(index)
                .execute(new ActionListener<DeleteIndexResponse>() {

                    @Override
                    public void onResponse(final DeleteIndexResponse response) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Deleted {} index.", index);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        logger.error("Failed to delete {} index.", e, index);
                    }
                });
    }

    private void writeDictionaryFile(final String index,
            final Queue<Tuple<String, Tuple<String, File>>> dictionaryQueue,
            final ActionListener<Void> listener) {
        final Tuple<String, Tuple<String, File>> dictionarySet = dictionaryQueue
                .poll();
        if (dictionarySet == null) {
            listener.onResponse(null);
            return;
        }

        final String type = dictionarySet.v1();
        final String path = dictionarySet.v2().v1();
        final File file = dictionarySet.v2().v2();
        final String absolutePath = file.getAbsolutePath();
        try (FileInputStream fis = new FileInputStream(file)) {
            final FileChannel fileChannel = fis.getChannel();

            final MappedByteBuffer buffer = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY, 0L, fileChannel.size());
            final ChannelBuffer channelBuffer = ChannelBuffers
                    .wrappedBuffer(buffer);

            final Map<String, Object> source = new HashMap<>();
            source.put(DictionaryConstants.PATH_FIELD, path);
            source.put(DictionaryConstants.ABSOLUTE_PATH_FIELD, absolutePath);
            source.put(DictionaryConstants.DATA_FIELD,
                    new ChannelBufferBytesReference(channelBuffer));
            client.prepareIndex(index, type).setSource(source)
                    .execute(new ActionListener<IndexResponse>() {

                        @Override
                        public void onResponse(final IndexResponse response) {
                            writeDictionaryFile(index, dictionaryQueue,
                                    listener);
                        }

                        @Override
                        public void onFailure(final Throwable e) {
                            listener.onFailure(e);
                        }

                    });
        } catch (final Exception e) {
            throw new DictionaryException("Failed to index " + type + ":"
                    + path + " to " + index, e);
        }
    }

    private XContentBuilder createDictionaryMappingBuilder(final String type)
            throws IOException {
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(type)//

                .startObject("_all")//
                .field("enabled", false)//
                .endObject()//

                .startObject("_source")//
                .field("enabled", false)//
                .endObject()//

                .startObject("properties")//

                // path
                .startObject(DictionaryConstants.PATH_FIELD)//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .field("store", true)//
                .endObject()//

                // absolute_path
                .startObject(DictionaryConstants.ABSOLUTE_PATH_FIELD)//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .field("store", true)//
                .endObject()//

                // data
                .startObject(DictionaryConstants.DATA_FIELD)//
                .field("type", "binary")//
                .field("store", true)//
                // .field("compress", true)//
                // .field("compress_threshold", -1)//
                .endObject()//

                .endObject()//
                .endObject()//
                .endObject();
        return mappingBuilder;
    }

    private void addDictionaryFile(
            final List<Tuple<String, File>> dictionaryList, final String value) {
        if (!value.startsWith("/")) {
            final File configFile = env.configFile();
            final File dictFileInConf = new File(configFile, value);
            if (dictFileInConf.exists()) {
                dictionaryList.add(new Tuple<String, File>(value,
                        dictFileInConf));
                return;
            }
        }

        final File dictFile = new File(value);
        if (dictFile.exists()) {
            dictionaryList.add(new Tuple<String, File>(value, dictFile));
        } else if (logger.isDebugEnabled()) {
            logger.debug("{} is not found.", value);
        }
    }

    private boolean isDictionaryFile(final String value) {
        for (final String fileExtension : fileExtensions) {
            if (value != null && value.endsWith("." + fileExtension)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onSnapshotFailure(final SnapshotId snapshotId, final Throwable t) {
        // nothing
    }

}
