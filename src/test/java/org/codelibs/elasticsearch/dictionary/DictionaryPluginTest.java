package org.codelibs.elasticsearch.dictionary;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import junit.framework.TestCase;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.junit.After;
import org.junit.Before;

public class DictionaryPluginTest extends TestCase {
    ElasticsearchClusterRunner runner;

    private File repositoryDir;

    private String repositoryName;

    private File[] userDictFiles;

    private File[] synonymFiles;

    private int numOfNode = 3;

    @Before
    public void setUp() throws Exception {
        repositoryDir = File.createTempFile("mysnapshot", "");
        repositoryDir.delete();
        repositoryDir.mkdirs();
        repositoryName = "myrepo";

        runner = new ElasticsearchClusterRunner();
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("index.number_of_replicas", 1);
            }
        }).build(
                newConfigs()
                        .clusterName("es-dict-" + System.currentTimeMillis())
                        .numOfNode(numOfNode).ramIndexStore());
        runner.ensureGreen();

        Node node = runner.node();
        node.client()
                .admin()
                .cluster()
                .preparePutRepository(repositoryName)
                .setType("fs")
                .setSettings(
                        ImmutableSettings.settingsBuilder().put("location",
                                repositoryDir.getAbsolutePath())).execute()
                .actionGet();

        userDictFiles = new File[numOfNode];
        synonymFiles = new File[numOfNode];
        for (int i = 0; i < numOfNode; i++) {
            String confPath = runner.getNode(i).settings().get("path.conf");
            userDictFiles[i] = new File(confPath, "userdict_ja.txt");
            updateDictionary(userDictFiles[i],
                    "関西国際空港,関西 国際 空港,カンサイ コクサイ クウコウ,カスタム名詞");

            synonymFiles[i] = new File(confPath, "synonym.txt");
            updateDictionary(synonymFiles[i], "i-pod, i pod => ipod");
        }

        runner.print("Repository: " + repositoryDir.getAbsolutePath());

    }

    private void updateDictionary(File file, String content)
            throws IOException, UnsupportedEncodingException,
            FileNotFoundException {
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(file), "UTF-8"))) {
            bw.write(content);
            bw.flush();
        }
    }

    @After
    public void tearDown() throws Exception {
        runner.close();
        runner.clean();
        repositoryDir.delete();
    }

    public void test_indexWithDictionaries() throws Exception {
        String index = "sample";
        String type = "data";

        // create an index
        final String indexSettings = "{\"index\":{\"analysis\":{"
                + "\"tokenizer\":{"//
                + "\"kuromoji_user_dict\":{\"type\":\"kuromoji_tokenizer\",\"mode\":\"extended\",\"discard_punctuation\":\"false\",\"user_dictionary\":\"userdict_ja.txt\"}"
                + "},"//
                + "\"filter\":{"//
                + "\"synonym\":{\"type\":\"synonym\",\"synonyms_path\":\"synonym.txt\"}"//
                + "},"//
                + "\"analyzer\":{"
                + "\"my_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"kuromoji_user_dict\",\"filter\":[\"synonym\"]}"
                + "}"//
                + "}}}";
        runner.createIndex(index,
                ImmutableSettings.builder().loadFromSource(indexSettings)
                        .build());
        //               runner.createIndex(index, null);
        runner.ensureYellow(index);

        // create a mapping
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(type)//
                .startObject("properties")//

                // id
                .startObject("id")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//

                // msg
                .startObject("msg")//
                .field("type", "string")//
                .endObject()//

                // order
                .startObject("order")//
                .field("type", "long")//
                .endObject()//

                // @timestamp
                .startObject("@timestamp")//
                .field("type", "date")//
                .endObject()//

                .endObject()//
                .endObject()//
                .endObject();
        runner.createMapping(index, type, mappingBuilder);

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, type,
                    String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\"test "
                            + i + "\",\"order\":" + i
                            + ",\"@timestamp\":\"2000-01-01T00:00:00\"}");
            assertTrue(indexResponse1.isCreated());
        }
        runner.flush();

        assertTrue(runner.indexExists(index));

        Client client = runner.client();

        String snapshotName = "snapshot";

        {
            CreateSnapshotResponse response = client.admin().cluster()
                    .prepareCreateSnapshot(repositoryName, snapshotName)
                    .setWaitForCompletion(true).execute().actionGet();
            SnapshotInfo snapshotInfo = response.getSnapshotInfo();
            assertEquals(0, snapshotInfo.failedShards());
        }

        Thread.sleep(5000); // TODO

        runner.deleteIndex(index);
        runner.flush();

        assertFalse(runner.indexExists(index));

        for (int i = 0; i < numOfNode; i++) {
            userDictFiles[i].delete();
            synonymFiles[i].delete();
        }

        for (int i = 0; i < numOfNode; i++) {
            assertFalse(userDictFiles[i].exists());
            assertFalse(synonymFiles[i].exists());
        }

        runner.ensureGreen();

        {
            RestoreSnapshotResponse response = client.admin().cluster()
                    .prepareRestoreSnapshot(repositoryName, snapshotName)
                    .setWaitForCompletion(true).execute().actionGet();
            RestoreInfo restoreInfo = response.getRestoreInfo();
            assertEquals(0, restoreInfo.failedShards());
        }

        assertTrue(runner.indexExists(index));

        for (int i = 0; i < numOfNode; i++) {
            assertTrue(userDictFiles[i].exists());
            assertTrue(synonymFiles[i].exists());
        }

    }

    public void test_indexWithoutDictionaries() throws Exception {
        String index = "sample";
        String type = "data";

        // create an index
        runner.createIndex(index, null);
        runner.ensureYellow(index);

        // create a mapping
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(type)//
                .startObject("properties")//

                // id
                .startObject("id")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//

                // msg
                .startObject("msg")//
                .field("type", "string")//
                .endObject()//

                // order
                .startObject("order")//
                .field("type", "long")//
                .endObject()//

                // @timestamp
                .startObject("@timestamp")//
                .field("type", "date")//
                .endObject()//

                .endObject()//
                .endObject()//
                .endObject();
        runner.createMapping(index, type, mappingBuilder);

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, type,
                    String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\"test "
                            + i + "\",\"order\":" + i
                            + ",\"@timestamp\":\"2000-01-01T00:00:00\"}");
            assertTrue(indexResponse1.isCreated());
        }
        runner.flush();

        assertTrue(runner.indexExists(index));

        Client client = runner.client();

        String snapshotName = "snapshot";

        {
            CreateSnapshotResponse response = client.admin().cluster()
                    .prepareCreateSnapshot(repositoryName, snapshotName)
                    .setWaitForCompletion(true).execute().actionGet();
            SnapshotInfo snapshotInfo = response.getSnapshotInfo();
            assertEquals(0, snapshotInfo.failedShards());
        }

        runner.deleteIndex(index);
        runner.flush();

        assertFalse(runner.indexExists(index));

        runner.ensureGreen();

        {
            RestoreSnapshotResponse response = client.admin().cluster()
                    .prepareRestoreSnapshot(repositoryName, snapshotName)
                    .setWaitForCompletion(true).execute().actionGet();
            RestoreInfo restoreInfo = response.getRestoreInfo();
            assertEquals(0, restoreInfo.failedShards());
        }

        assertTrue(runner.indexExists(index));

    }
}
