package org.codelibs.elasticsearch.dictionary.filter;

import org.codelibs.elasticsearch.dictionary.service.DictionarySnapshotService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotInfo;

public class CreateSnapshotActionFilter extends AbstractComponent implements
        ActionFilter {

    private int order;

    private DictionarySnapshotService dictionarySnapshotService;

    private ClusterService clusterService;

    @Inject
    public CreateSnapshotActionFilter(final Settings settings, final ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;

        order = settings.getAsInt("indices.dictionary.filter.order", 1);
    }

    public void setDictionarySnapshotService(
            final DictionarySnapshotService dictionarySnapshotService) {
        this.dictionarySnapshotService = dictionarySnapshotService;
    }

    @Override
    public int order() {
        return order;
    }

    @Override
    public void apply(final String action,
            @SuppressWarnings("rawtypes") final ActionRequest request,
            @SuppressWarnings("rawtypes") final ActionListener listener,
            final ActionFilterChain chain) {
        if (!CreateSnapshotAction.NAME.equals(action)) {
            chain.proceed(action, request, listener);
        } else {
            final CreateSnapshotRequest createSnapshotRequest = (CreateSnapshotRequest) request;
            chain.proceed(action, request, new ActionListenerWrapper<>(
                    listener, new SnapshotId(
                            createSnapshotRequest.repository(),
                            createSnapshotRequest.snapshot())));
        }
    }

    @Override
    public void apply(final String action, final ActionResponse response,
            @SuppressWarnings("rawtypes") final ActionListener listener,
            final ActionFilterChain chain) {
        if (!CreateSnapshotAction.NAME.equals(action) || !clusterService.state().nodes().localNodeMaster()) {
            chain.proceed(action, response, listener);
        } else {
            final CreateSnapshotResponse createSnapshotResponse = (CreateSnapshotResponse) response;
            final SnapshotInfo snapshotInfo = createSnapshotResponse
                    .getSnapshotInfo();
            dictionarySnapshotService.createDictionarySnapshot(
                    ((ActionListenerWrapper<?>) listener).getSnapshotId(),
                    snapshotInfo, new ActionListener<Void>() {

                        @Override
                        public void onResponse(final Void resp) {
                            chain.proceed(action, response, listener);
                        }

                        @Override
                        public void onFailure(final Throwable e) {
                            listener.onFailure(e);
                        }
                    });
        }
    }

    private static class ActionListenerWrapper<T> implements ActionListener<T> {

        private ActionListener<T> listener;

        private SnapshotId snapshotId;

        ActionListenerWrapper(final ActionListener<T> listener,
                final SnapshotId snapshotId) {
            this.listener = listener;
            this.snapshotId = snapshotId;

        }

        @Override
        public void onResponse(final T response) {
            listener.onResponse(response);
        }

        @Override
        public void onFailure(final Throwable e) {
            listener.onFailure(e);
        }

        public SnapshotId getSnapshotId() {
            return snapshotId;
        }

    }
}
