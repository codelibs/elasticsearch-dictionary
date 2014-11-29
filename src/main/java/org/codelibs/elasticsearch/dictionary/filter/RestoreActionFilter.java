package org.codelibs.elasticsearch.dictionary.filter;

import org.codelibs.elasticsearch.dictionary.service.DictionaryRestoreService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class RestoreActionFilter extends AbstractComponent implements
        ActionFilter {

    private int order;

    private DictionaryRestoreService dictionaryRestoreService;

    @Inject
    public RestoreActionFilter(final Settings settings) {
        super(settings);

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
    public void apply(final String action,
            @SuppressWarnings("rawtypes") final ActionRequest request,
            @SuppressWarnings("rawtypes") final ActionListener listener,
            final ActionFilterChain chain) {
        if (!RestoreSnapshotAction.NAME.equals(action)) {
            chain.proceed(action, request, listener);
        } else {
            final RestoreSnapshotRequest restoreRequest = (RestoreSnapshotRequest) request;
            dictionaryRestoreService.restoreDictionarySnapshot(
                    restoreRequest.repository(), restoreRequest.snapshot(),
                    restoreRequest.indices(), new ActionListener<Void>() {

                        @Override
                        public void onResponse(final Void response) {
                            chain.proceed(action, request, listener);
                        }

                        @Override
                        public void onFailure(final Throwable e) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(
                                        "Failed to restore dictionaries for {} snapshot in {} repository.",
                                        e, restoreRequest.repository(),
                                        restoreRequest.snapshot());
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
