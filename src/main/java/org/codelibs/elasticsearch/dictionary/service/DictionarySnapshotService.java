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
import org.codelibs.elasticsearch.dictionary.filter.CreateSnapshotActionFilter;
import org.codelibs.elasticsearch.dictionary.filter.DeleteSnapshotActionFilter;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.snapshots.SnapshotsService.CreateSnapshotListener;
import org.elasticsearch.snapshots.SnapshotsService.SnapshotRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class DictionarySnapshotService extends AbstractComponent {

    private static final String ACTION_DICTIONARY_SNAPSHOT_DELETE = "internal:cluster/snapshot/delete_dictionary";

    private SnapshotsService snapshotsService;

    private IndicesService indicesService;

    private Environment env;

    private String[] fileExtensions;

    private String dictionaryIndex;

    private Client client;

    private TimeValue masterNodeTimeout;

    private TransportService transportService;

    private ClusterService clusterService;

    @Inject
    public DictionarySnapshotService(final Settings settings, final Client client, final Environment env,
            final ClusterService clusterService, final IndicesService indicesService, final TransportService transportService,
            final SnapshotsService snapshotsService, final ActionFilters filters) {
        super(settings);
        this.client = client;
        this.env = env;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.transportService = transportService;
        this.snapshotsService = snapshotsService;

        dictionaryIndex = settings.get("dictionary.index", ".dictionary");
        fileExtensions = settings.getAsArray("directory.file_extensions",
                new String[] { "txt" });
        masterNodeTimeout = settings.getAsTime(
                "dictionary.snapshot.master_node_timeout",
                TimeValue.timeValueSeconds(30));

        for (final ActionFilter filter : filters.filters()) {
            if (filter instanceof CreateSnapshotActionFilter) {
                ((CreateSnapshotActionFilter) filter)
                        .setDictionarySnapshotService(this);
                if (logger.isDebugEnabled()) {
                    logger.debug("Set CreateSnapshotActionFilter to " + filter);
                }
            } else if (filter instanceof DeleteSnapshotActionFilter) {
                ((DeleteSnapshotActionFilter) filter)
                        .setDictionarySnapshotService(this);
                if (logger.isDebugEnabled()) {
                    logger.debug("Set DeleteSnapshotActionFilter to " + filter);
                }
            }
        }

        transportService.registerRequestHandler(ACTION_DICTIONARY_SNAPSHOT_DELETE,
                DeleteDictionaryRequest.class, ThreadPool.Names.SNAPSHOT,
                new DeleteDictionaryRequestHandler());
    }

    private DiscoveryNode getMasterNode() {
        final ClusterState clusterState = clusterService.state();
        final DiscoveryNodes nodes = clusterState.nodes();
        return nodes.localNodeMaster() ? null : nodes.masterNode();
    }

    public void deleteDictionarySnapshot(final String repository, final String snapshot, final ActionListener<Void> listener) {
        DiscoveryNode masterNode = getMasterNode();
        if (masterNode == null) {
            deleteDictionarySnapshotOnMaster(repository, snapshot, listener);
        } else {
            transportService.sendRequest(masterNode, ACTION_DICTIONARY_SNAPSHOT_DELETE, new DeleteDictionaryRequest(repository,snapshot),
                    new TransportResponseHandler<DeleteDictionaryResponse>() {

                        @Override
                        public DeleteDictionaryResponse newInstance() {
                             return new DeleteDictionaryResponse();
                        }

                        @Override
                        public void handleResponse(DeleteDictionaryResponse response) {
                            if (response.isAcknowledged()) {
                                listener.onResponse(null);
                            } else {
                                listener.onFailure(new DictionaryException("Could not delete " + repository + ":" + snapshot + ". "
                                        + response.message));
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            listener.onFailure(exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.GENERIC;
                        }
                    });
        }
    }

    public void deleteDictionarySnapshotOnMaster(final String repository,
                final String snapshot, final ActionListener<Void> listener) {
        final String dictionarySnapshot = snapshot + dictionaryIndex + "_"
                + repository + "_" + snapshot;
        client.admin().cluster().prepareGetSnapshots(repository)
                .setSnapshots(dictionarySnapshot)
                .execute(new ActionListener<GetSnapshotsResponse>() {

                    @Override
                    public void onResponse(GetSnapshotsResponse response) {
                        client.admin()
                                .cluster()
                                .prepareDeleteSnapshot(repository,
                                        dictionarySnapshot)
                                .setMasterNodeTimeout(masterNodeTimeout)
                                .execute(
                                        new ActionListener<DeleteSnapshotResponse>() {

                                            @Override
                                            public void onResponse(
                                                    DeleteSnapshotResponse response) {
                                                if (logger.isDebugEnabled()) {
                                                    logger.debug(
                                                            "Deleted {} snapshot.",
                                                            dictionarySnapshot);
                                                }
                                                listener.onResponse(null);
                                            }

                                            @Override
                                            public void onFailure(Throwable e) {
                                                listener.onFailure(e);
                                            }
                                        });
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        if (e instanceof SnapshotMissingException) {
                            listener.onResponse(null);
                        } else {
                            listener.onFailure(new DictionaryException(
                                    "Failed to find " + dictionarySnapshot
                                            + " snapshot.", e));
                        }
                    }
                });
    }

    public void createDictionarySnapshot(final SnapshotId snapshotId,
            final SnapshotInfo snapshot, final ActionListener<Void> listener) {

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
                        logger.debug("{} => {} is found in {}.", dictInfo.v1(),
                                dictInfo.v2(), entry.getKey());
                    }
                }
            }
            snapshotDictionaryIndex(snapshotId, indexDictionaryMap,
                    new ActionListener<Void>() {
                        @Override
                        public void onResponse(final Void response) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(
                                        "Created {} snapshot.",
                                        dictionaryIndex + "_"
                                                + snapshotId.getRepository()
                                                + "_"
                                                + snapshotId.getSnapshot());
                            }
                            listener.onResponse(null);
                        }

                        @Override
                        public void onFailure(final Throwable e) {
                            listener.onFailure(e);
                        }
                    });
        } else {
            listener.onResponse(null);
        }
    }

    private void snapshotDictionaryIndex(final SnapshotId snapshotId,
            final Map<String, List<Tuple<String, File>>> indexDictionaryMap,
            final ActionListener<Void> listener) {
        final String index = dictionaryIndex + "_" + snapshotId.getRepository()
                + "_" + snapshotId.getSnapshot();
        if (logger.isDebugEnabled()) {
            logger.debug("Creating dictionary index: {}", index);
        }
        try {
            final CreateIndexRequestBuilder builder = client.admin().indices()
                    .prepareCreate(index)
                    .setSettings(createDictionarySettingBuilder());
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
                                indexDictionaryMap, listener);
                    } catch (final Exception e) {
                        deleteDictionaryIndex(index);
                        throw e;
                    }
                }

                @Override
                public void onFailure(final Throwable e) {
                    listener.onFailure(e);
                }
            });
        } catch (final Exception e) {
            listener.onFailure(e);
        }
    }

    private void writeDictionaryIndex(final String index,
            final SnapshotId snapshotId,
            final Map<String, List<Tuple<String, File>>> indexDictionaryMap,
            final ActionListener<Void> listener) {
        if (logger.isDebugEnabled()) {
            logger.debug("Writing dictionary index: {}", index);
        }
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
                flushDictionaryIndex(index, snapshotId, listener);
            }

            @Override
            public void onFailure(final Throwable e) {
                listener.onFailure(e);
                deleteDictionaryIndex(index);
            }
        });
    }

    private void flushDictionaryIndex(final String index,
            final SnapshotId snapshotId, final ActionListener<Void> listener) {
        if (logger.isDebugEnabled()) {
            logger.debug("Flushing dictionary index: {}", index);
        }
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
                                                        snapshotId, listener);
                                            }

                                            @Override
                                            public void onFailure(
                                                    final Throwable e) {
                                                listener.onFailure(e);
                                                deleteDictionaryIndex(index);
                                            }
                                        });
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        listener.onFailure(e);
                        deleteDictionaryIndex(index);
                    }

                });
    }

    private void createDictionarySnapshot(final String index, final SnapshotId snapshotId, final ActionListener<Void> listener) {
        final String repository = snapshotId.getRepository();
        final String snapshot = snapshotId.getSnapshot();
        final String name = snapshot + index;
        if (logger.isDebugEnabled()) {
            logger.debug("Creating dictionary snapshot: {}", name);
        }
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
                                    listener.onResponse(null);
                                    deleteDictionaryIndex(index);
                                    snapshotsService.removeListener(this);
                                }
                            }

                            @Override
                            public void onSnapshotFailure(
                                    final SnapshotId snapshotId2,
                                    final Throwable t) {
                                if (snapshotId.equals(snapshotId)) {
                                    listener.onFailure(t);
                                    deleteDictionaryIndex(index);
                                    snapshotsService.removeListener(this);
                                }
                            }
                        });
            }

            @Override
            public void onFailure(final Throwable t) {
                listener.onFailure(t);
                deleteDictionaryIndex(index);
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

    private XContentBuilder createDictionarySettingBuilder() throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder()//
                .startObject()//

                .startObject("index")//
                .field("number_of_shards", 1)//
                .field("number_of_replicas", 0)//
                .endObject()//

                .endObject();
        return builder;
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
            final File dictFileInConf = env.configFile().resolve(value).toFile();
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

    class DeleteDictionaryRequestHandler
            implements TransportRequestHandler<DeleteDictionaryRequest> {

        @Override
        public void messageReceived(final DeleteDictionaryRequest request, final TransportChannel channel) throws Exception {
            deleteDictionarySnapshotOnMaster(request.repository, request.snapshot, new ActionListener<Void>() {

                @Override
                public void onResponse(Void response) {
                    try {
                        channel.sendResponse(new DeleteDictionaryResponse(true));
                    } catch (IOException e) {
                        throw ExceptionsHelper.convertToRuntime(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(new DeleteDictionaryResponse(false, e.getMessage()));
                    } catch (IOException e1) {
                        throw ExceptionsHelper.convertToRuntime(e);
                    }
                }
            });
        }
    }

    public static class DeleteDictionaryRequest extends TransportRequest {

        private String repository;
        private String snapshot;

        public DeleteDictionaryRequest() {
        }

        public DeleteDictionaryRequest(String repository, String snapshot) {
            this.repository = repository;
            this.snapshot = snapshot;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            repository = in.readString();
            snapshot = in.readString();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeString(snapshot);
        }
    }

    private static class DeleteDictionaryResponse extends AcknowledgedResponse {

        private String message;

        DeleteDictionaryResponse() {
        }

        DeleteDictionaryResponse(final boolean acknowledged) {
            super(acknowledged);
        }

        DeleteDictionaryResponse(final boolean acknowledged, final String message) {
            super(acknowledged);
            this.message = message;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
            if (!isAcknowledged()) {
                message = in.readString();
            }
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
            if (!isAcknowledged()) {
                out.writeString(message);
            }
        }
    }
}
