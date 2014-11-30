package org.codelibs.elasticsearch.dictionary;

import java.util.Collection;

import org.codelibs.elasticsearch.dictionary.filter.RestoreActionFilter;
import org.codelibs.elasticsearch.dictionary.filter.SnapshotActionFilter;
import org.codelibs.elasticsearch.dictionary.module.DictionaryModule;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;

public class DictionaryPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "DictionaryPlugin";
    }

    @Override
    public String description() {
        return "This plugin manages dictionary files.";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists
                .newArrayList();
        modules.add(DictionaryModule.class);
        return modules;
    }

    public void onModule(final ActionModule module) {
        module.registerFilter(SnapshotActionFilter.class);
        module.registerFilter(RestoreActionFilter.class);
    }

}
