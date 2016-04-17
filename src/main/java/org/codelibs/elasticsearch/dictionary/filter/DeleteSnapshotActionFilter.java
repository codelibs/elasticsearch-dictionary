package org.codelibs.elasticsearch.dictionary.filter;

import org.codelibs.elasticsearch.dictionary.service.DictionarySnapshotService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

public class DeleteSnapshotActionFilter extends AbstractComponent implements
        ActionFilter {

    private int order;

    private DictionarySnapshotService dictionarySnapshotService;

    @Inject
    public DeleteSnapshotActionFilter(final Settings settings) {
        super(settings);

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
    public void apply(final Task task, final String action,
            @SuppressWarnings("rawtypes") final ActionRequest request,
            @SuppressWarnings("rawtypes") final ActionListener listener,
            final ActionFilterChain chain) {
        if (!DeleteSnapshotAction.NAME.equals(action)) {
            chain.proceed(task, action, request, listener);
        } else {
            final DeleteSnapshotRequest deleteSnapshotRequest = (DeleteSnapshotRequest) request;
            dictionarySnapshotService.deleteDictionarySnapshot(
                    deleteSnapshotRequest.repository(),
                    deleteSnapshotRequest.snapshot(),
                    new ActionListener<Void>() {

                        @Override
                        public void onResponse(Void response) {
                            chain.proceed(task, action, request, listener);
                        }

                        @Override
                        public void onFailure(Throwable e) {
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
