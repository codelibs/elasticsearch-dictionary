package org.codelibs.elasticsearch.dictionary.filter;

import org.codelibs.elasticsearch.dictionary.service.DictionaryRestoreService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

public class RestoreSnapshotActionFilter extends AbstractComponent implements
        ActionFilter {

    private int order;

    private DictionaryRestoreService dictionaryRestoreService;

    private ClusterService clusterService;

    @Inject
    public RestoreSnapshotActionFilter(final Settings settings, final ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;

        order = settings.getAsInt("indices.dictionary.filter.order", 1);
    }

    public void setDictionaryRestoreService(
            final DictionaryRestoreService dictionaryRestoreService) {
        this.dictionaryRestoreService = dictionaryRestoreService;
    }

    @Override
    public int order() {
        return order;
    }

    @Override
    public void apply(final Task task, final String action,
            @SuppressWarnings("rawtypes") final ActionRequest request,
            @SuppressWarnings("rawtypes") final ActionListener listener,
            final ActionFilterChain chain) {
        if (!RestoreSnapshotAction.NAME.equals(action) || !clusterService.state().nodes().localNodeMaster()) {
            chain.proceed(task, action, request, listener);
        } else {
            final RestoreSnapshotRequest restoreSnapshotRequest = (RestoreSnapshotRequest) request;
            dictionaryRestoreService.restoreDictionarySnapshot(
                    restoreSnapshotRequest.repository(), restoreSnapshotRequest.snapshot(),
                    restoreSnapshotRequest.indices(), new ActionListener<Void>() {

                        @Override
                        public void onResponse(final Void response) {
                            chain.proceed(task, action, request, listener);
                        }

                        @Override
                        public void onFailure(final Throwable e) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(
                                        "Failed to restore dictionaries for {} snapshot in {} repository.",
                                        e, restoreSnapshotRequest.repository(),
                                        restoreSnapshotRequest.snapshot());
                            }
                            listener.onFailure(e);
                        }
                    });
        }
    }

    @Override
    public void apply(final String action, final ActionResponse response,
            @SuppressWarnings("rawtypes") final ActionListener listener,
            final ActionFilterChain chain) {
        chain.proceed(action, response, listener);
    }

}
