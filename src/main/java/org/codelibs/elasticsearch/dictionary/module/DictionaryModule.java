package org.codelibs.elasticsearch.dictionary.module;

import org.codelibs.elasticsearch.dictionary.service.DictionaryRestoreService;
import org.codelibs.elasticsearch.dictionary.service.DictionarySnapshotService;
import org.elasticsearch.common.inject.AbstractModule;

public class DictionaryModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(DictionarySnapshotService.class).asEagerSingleton();
        bind(DictionaryRestoreService.class).asEagerSingleton();
    }

}
