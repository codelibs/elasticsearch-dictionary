package org.codelibs.elasticsearch.dictionary.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codelibs.elasticsearch.dictionary.DictionaryConstants;
import org.codelibs.elasticsearch.dictionary.DictionaryException;
import org.codelibs.elasticsearch.dictionary.filter.RestoreSnapshotActionFilter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.RestoreService.RestoreRequest;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import com.google.common.collect.UnmodifiableIterator;

public class DictionaryRestoreService extends AbstractComponent {

    public static final String ACTION_RESTORE_DICTIONERY = "internal:index/dictionary/restore";

    private Client client;

    private Environment env;

    private ClusterService clusterService;

    private RestoreService restoreService;

    private String dictionaryIndex;

    private TimeValue masterNodeTimeout;

    private int maxNumOfDictionaries;

    private TransportService transportService;

    @Inject
    public DictionaryRestoreService(final Settings settings,
            final Client client, final Environment env,
            final ClusterService clusterService,
            final TransportService transportService,
            final RestoreService restoreService, final ActionFilters filters) {
        super(settings);
        this.client = client;
        this.env = env;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.restoreService = restoreService;

        dictionaryIndex = settings.get("dictionary.index", ".dictionary");
        masterNodeTimeout = settings.getAsTime(
                "dictionary.restore.master_node_timeout",
                TimeValue.timeValueSeconds(30));
        maxNumOfDictionaries = settings.getAsInt(
                "dictionary.restore.max_num_of_dictionaries", 100);

        for (final ActionFilter filter : filters.filters()) {
            if (filter instanceof RestoreSnapshotActionFilter) {
                ((RestoreSnapshotActionFilter) filter)
                        .setDictionaryRestoreService(this);
                if (logger.isDebugEnabled()) {
                    logger.debug("Set DictionaryRestoreService to " + filter);
                }
            }
        }

        transportService.registerRequestHandler(ACTION_RESTORE_DICTIONERY,
                RestoreDictionaryRequest.class, ThreadPool.Names.SNAPSHOT,
                new RestoreDictionaryRequestHandler());
    }

    public void restoreDictionarySnapshot(final String repository,
            final String snapshot, final String[] indices,
            final ActionListener<Void> listener) {
        final String dictionarySnapshot = snapshot + dictionaryIndex + "_"
                + repository + "_" + snapshot;
        client.admin().cluster().prepareGetSnapshots(repository)
                .addSnapshots(dictionarySnapshot)
                .execute(new ActionListener<GetSnapshotsResponse>() {

                    @Override
                    public void onResponse(final GetSnapshotsResponse response) {
                        final List<SnapshotInfo> snapshots = response
                                .getSnapshots();
                        if (!snapshots.isEmpty()) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(
                                        "Restoring {} dictionaries in {} repository.",
                                        repository, snapshot);
                            }
                            restoreDictionaryIndex(repository, snapshot,
                                    dictionarySnapshot, indices, listener);
                        } else {
                            if (logger.isDebugEnabled()) {
                                logger.debug(
                                        "{} snapshot in {} repository does not have dictionaries.",
                                        repository, snapshot);
                            }
                            listener.onResponse(null);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable e) {
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

    private void restoreDictionaryIndex(final String repository,
            final String snapshot, final String dictionarySnapshot,
            final String[] indices, final ActionListener<Void> listener) {
        final RestoreRequest request = new RestoreRequest(
                "restore dictionary snapshot[" + dictionarySnapshot + "]",
                repository, dictionarySnapshot, Strings.EMPTY_ARRAY,
                IndicesOptions.strictExpandOpen(), null, null,
                Settings.EMPTY, masterNodeTimeout, false, false,
                false, Settings.EMPTY, Strings.EMPTY_ARRAY);
        restoreService.restoreSnapshot(request,
                new ActionListener<RestoreInfo>() {
                    @Override
                    public void onResponse(final RestoreInfo response) {
                        restoreService
                                .addListener(new ActionListener<RestoreService.RestoreCompletionResponse>() {
                                    SnapshotId snapshotId = new SnapshotId(
                                            repository, dictionarySnapshot);

                                    @Override
                                    public void onResponse(
                                            final RestoreService.RestoreCompletionResponse restoreCompletionResponse) {
                                        if (snapshotId
                                                .equals(restoreCompletionResponse
                                                        .getSnapshotId())) {
                                            try {
                                                final List<String> dictionaryIndices = restoreCompletionResponse
                                                        .getRestoreInfo()
                                                        .indices();
                                                if (logger.isDebugEnabled()) {
                                                    logger.debug(
                                                            "Snapshot {} has {}.",
                                                            snapshotId,
                                                            dictionaryIndices);
                                                }
                                                if (dictionaryIndices.isEmpty()) {
                                                    listener.onFailure(new DictionaryException(
                                                            dictionarySnapshot
                                                                    + " snapshot does not have indices."));
                                                } else {
                                                    restoreDictionaryFiles(
                                                            indices,
                                                            dictionaryIndices
                                                                    .toArray(new String[dictionaryIndices
                                                                            .size()]),
                                                            listener);
                                                }
                                            } finally {
                                                restoreService
                                                        .removeListener(this);
                                            }
                                        }
                                    }

                                    @Override
                                    public void onFailure(final Throwable e) {
                                        listener.onFailure(new DictionaryException(
                                                "Failed to restore "
                                                        + dictionarySnapshot
                                                        + " snapshot.", e));
                                    }
                                });
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        listener.onFailure(new DictionaryException(
                                "Failed to restore " + dictionarySnapshot
                                        + " snapshot.", e));
                    }
                });
    }

    private void restoreDictionaryFiles(final String[] indices,
            final String[] dictionaryIndices,
            final ActionListener<Void> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        final UnmodifiableIterator<DiscoveryNode> nodesIt = nodes.dataNodes()
                .valuesIt();
        restoreDictionaryFile(nodesIt, dictionaryIndices, listener);
    }

    private void restoreDictionaryFile(
            final UnmodifiableIterator<DiscoveryNode> nodesIt,
            final String[] dictionaryIndices,
            final ActionListener<Void> listener) {
        if (!nodesIt.hasNext()) {
            // wrote all dictionaries in all data node
            listener.onResponse(null);
            for (final String index : dictionaryIndices) {
                deleteDictionaryIndex(index);
            }
        } else {
            final DiscoveryNode node = nodesIt.next();
            transportService.sendRequest(node, ACTION_RESTORE_DICTIONERY,
                    new RestoreDictionaryRequest(dictionaryIndices),
                    new TransportResponseHandler<RestoreDictionaryResponse>() {

                        @Override
                        public RestoreDictionaryResponse newInstance() {
                            return new RestoreDictionaryResponse();
                        }

                        @Override
                        public void handleResponse(
                                final RestoreDictionaryResponse response) {
                            restoreDictionaryFile(nodesIt, dictionaryIndices,
                                    listener);
                        }

                        @Override
                        public void handleException(final TransportException exp) {
                            listener.onFailure(exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SNAPSHOT;
                        }
                    });
        }
    }

    private boolean writeDictionaryFile(final BytesReference data,
            final String path) {
        final File file = new File(path);
        file.getParentFile().mkdirs();
        try (FileOutputStream fos = new FileOutputStream(file)) {
            final FileChannel channel = fos.getChannel();
            channel.write(data.toChannelBuffer().toByteBuffer());
            if (logger.isDebugEnabled()) {
                logger.debug("Wrote a dictionary file {}.", path);
            }
            return true;
        } catch (final Exception e) {
            logger.warn("Failed to write {} dictionary file.", e, file);
        }
        return false;
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

    class RestoreDictionaryRequestHandler
            extends TransportRequestHandler<RestoreDictionaryRequest> {

        @Override
        public void messageReceived(final RestoreDictionaryRequest request,
                final TransportChannel channel) throws Exception {
            client.prepareSearch(request.indices())
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(maxNumOfDictionaries)
                    .setFrom(0)
                    .addFields(DictionaryConstants.PATH_FIELD,
                            DictionaryConstants.ABSOLUTE_PATH_FIELD,
                            DictionaryConstants.DATA_FIELD)
                    .execute(new ActionListener<SearchResponse>() {

                        @Override
                        public void onResponse(final SearchResponse response) {
                            final SearchHits hits = response.getHits();
                            if (hits.hits().length != hits.getTotalHits()) {
                                logger.warn(
                                        "{} dictionary files are found, but there are {} files. "
                                                + "{} dictionary files are ignored.",
                                        hits.hits().length,
                                        hits.getTotalHits(),
                                        hits.getTotalHits()
                                                - hits.hits().length);
                            }

                            final Set<String> indices = new HashSet<String>();
                            for (final SearchHit hit : hits.hits()) {
                                indices.add(hit.getType());

                                final Map<String, SearchHitField> fields = hit
                                        .getFields();

                                final SearchHitField dataField = fields
                                        .get(DictionaryConstants.DATA_FIELD);
                                final BytesReference data = dataField
                                        .getValue();

                                final SearchHitField absolutePathField = fields
                                        .get(DictionaryConstants.ABSOLUTE_PATH_FIELD);
                                final String absolutePath = absolutePathField
                                        .getValue();

                                final SearchHitField pathField = fields
                                        .get(DictionaryConstants.PATH_FIELD);
                                final String path = pathField.getValue();

                                if (!path.startsWith("/")) {
                                    final File file = env.configFile()
                                            .resolve(path).toFile();
                                    if (!writeDictionaryFile(data,
                                            file.getAbsolutePath())) {
                                        logger.warn(
                                                "Failed to write {} dictionary file.",
                                                file.getAbsolutePath());
                                    }
                                } else if (!writeDictionaryFile(data,
                                        absolutePath)) {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug(
                                                "Failed to write {}. Retry to $ES_CONF/{}.",
                                                absolutePath, path);
                                    }
                                    final File file = env.configFile()
                                            .resolve(path).toFile();
                                    if (!writeDictionaryFile(data,
                                            file.getAbsolutePath())) {
                                        logger.warn(
                                                "Failed to write {} dictionary file.",
                                                file.getAbsolutePath());
                                    }
                                }
                            }

                            sendResponse(channel, true, null);
                        }

                        @Override
                        public void onFailure(final Throwable e) {
                            sendResponse(channel, false, e.getMessage());
                            if (logger.isDebugEnabled()) {
                                logger.debug(
                                        "Failed to restore dictionary files.",
                                        e);
                            }
                        }

                        private void sendResponse(
                                final TransportChannel channel,
                                final boolean acknowledged, final String message) {
                            try {
                                channel.sendResponse(new RestoreDictionaryResponse(
                                        acknowledged, message));
                            } catch (final IOException e) {
                                try {
                                    channel.sendResponse(new RestoreDictionaryResponse(
                                            false, e.getMessage()));
                                } catch (final IOException e1) {
                                    logger.error("Failed to send a response.",
                                            e);
                                }
                            }
                        }

                    });

        }
    }

    public static class RestoreDictionaryRequest extends TransportRequest {

        private String[] indices;

        public RestoreDictionaryRequest() {
        }

        public RestoreDictionaryRequest(final String[] indices) {
            this.indices = indices;
        }

        public String[] indices() {
            return indices;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            indices = in.readStringArray();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
        }
    }

    private static class RestoreDictionaryResponse extends AcknowledgedResponse {
        String message;

        RestoreDictionaryResponse() {
        }

        RestoreDictionaryResponse(final boolean acknowledged,
                final String message) {
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
