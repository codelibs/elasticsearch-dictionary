package org.codelibs.elasticsearch.dictionary.service;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codelibs.elasticsearch.dictionary.DictionaryConstants;
import org.codelibs.elasticsearch.dictionary.DictionaryException;
import org.codelibs.elasticsearch.dictionary.filter.RestoreActionFilter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
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

public class DictionaryRestoreService extends AbstractComponent {

    private RestoreService restoreService;

    private Environment env;

    private String dictionaryIndex;

    private Client client;

    private TimeValue masterNodeTimeout;

    private int maxNumOfDictionaries;

    @Inject
    public DictionaryRestoreService(final Settings settings,
            final Client client, final Environment env,
            final RestoreService restoreService, final ActionFilters filters) {
        super(settings);
        this.client = client;
        this.env = env;
        this.restoreService = restoreService;

        dictionaryIndex = settings.get("dictionary.index", ".dictionary");
        masterNodeTimeout = settings.getAsTime(
                "dictionary.restore.master_node_timeout",
                TimeValue.timeValueSeconds(30));
        maxNumOfDictionaries = settings.getAsInt(
                "dictionary.restore.max_num_of_dictionaries", 100);

        for (final ActionFilter filter : filters.filters()) {
            if (filter instanceof RestoreActionFilter) {
                ((RestoreActionFilter) filter)
                        .setDictionaryRestoreService(this);
                if (logger.isDebugEnabled()) {
                    logger.debug("Set DynamicRanker to " + filter);
                }
            }
        }
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
                        final ImmutableList<SnapshotInfo> snapshots = response
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
                        listener.onFailure(new DictionaryException(
                                "Failed to find " + dictionarySnapshot
                                        + " snapshot.", e));
                    }
                });
    }

    private void restoreDictionaryIndex(final String repository,
            final String snapshot, final String dictionarySnapshot,
            final String[] indices, final ActionListener<Void> listener) {
        final RestoreRequest request = new RestoreRequest(
                "restore dictionary snapshot", repository, dictionarySnapshot,
                Strings.EMPTY_ARRAY, IndicesOptions.strictExpandOpen(), null,
                null, ImmutableSettings.EMPTY, masterNodeTimeout, false, false,
                false);
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
                                            final ImmutableList<String> dictionaryIndices = restoreCompletionResponse
                                                    .getRestoreInfo().indices();
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
                                                restoreIndexFiles(
                                                        indices,
                                                        dictionaryIndices
                                                                .toArray(new String[dictionaryIndices
                                                                        .size()]),
                                                        listener);
                                            }
                                            restoreService.removeListener(this);
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

    private void restoreIndexFiles(final String[] indices,
            final String[] dictionaryIndices,
            final ActionListener<Void> listener) {
        client.prepareSearch(dictionaryIndices)
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
                                    hits.hits().length, hits.getTotalHits(),
                                    hits.getTotalHits() - hits.hits().length);
                        }

                        final Set<String> indices = new HashSet<String>();
                        for (final SearchHit hit : hits.hits()) {
                            indices.add(hit.getType());

                            final Map<String, SearchHitField> fields = hit
                                    .getFields();

                            final SearchHitField dataField = fields
                                    .get(DictionaryConstants.DATA_FIELD);
                            final BytesReference data = dataField.getValue();

                            final SearchHitField absolutePathField = fields
                                    .get(DictionaryConstants.ABSOLUTE_PATH_FIELD);
                            final String absolutePath = absolutePathField
                                    .getValue();
                            if (!writeDictionaryFile(data, absolutePath)) {
                                final SearchHitField pathField = fields
                                        .get(DictionaryConstants.PATH_FIELD);
                                final String path = pathField.getValue();
                                if (logger.isDebugEnabled()) {
                                    logger.debug(
                                            "Failed to write {}. Retry to $ES_CONF/{}.",
                                            absolutePath, path);
                                }
                                final File file = new File(env.configFile(),
                                        path);
                                if (!writeDictionaryFile(data,
                                        file.getAbsolutePath())) {
                                    logger.warn(
                                            "Failed to write {} dictionary file.",
                                            file.getAbsolutePath());
                                }
                            }
                        }

                        for (final String index : dictionaryIndices) {
                            deleteDictionaryIndex(index);
                        }

                        listener.onResponse(null);
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        listener.onFailure(new DictionaryException(
                                "Failed to restore dictionaries.", e));
                        for (final String index : dictionaryIndices) {
                            deleteDictionaryIndex(index);
                        }
                    }
                });
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

}
