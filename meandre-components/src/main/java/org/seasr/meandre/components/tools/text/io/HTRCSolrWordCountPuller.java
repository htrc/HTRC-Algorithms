package org.seasr.meandre.components.tools.text.io;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.meandre.annotations.Component;
import org.meandre.annotations.Component.Licenses;
import org.meandre.annotations.ComponentInput;
import org.meandre.annotations.ComponentOutput;
import org.meandre.annotations.ComponentProperty;
import org.meandre.core.ComponentContext;
import org.meandre.core.ComponentContextProperties;
import org.meandre.core.system.components.ext.StreamInitiator;
import org.meandre.core.system.components.ext.StreamTerminator;
import org.seasr.datatypes.core.BasicDataTypesTools;
import org.seasr.datatypes.core.DataTypeParser;
import org.seasr.datatypes.core.Names;
import org.seasr.meandre.components.abstracts.AbstractStreamingExecutableComponent;

import edu.indiana.d2i.htrc.clients.solr.HTRCSolrClient;

@Component(
        creator = "Jiaan Zeng",
        description = "Pull word count from Solr.",
        name = "HTRC Solr Word Count Puller",
        rights = Licenses.UofINCSA,
        tags = "text",
        dependency = {"protobuf-java-2.2.0.jar", "dataapi-client-0.6.2.jar", "pairtree-1.1.1.jar"},
        baseURL = "meandre://seasr.org/components/foundry/"
)
public class HTRCSolrWordCountPuller extends AbstractStreamingExecutableComponent {
    //------------------------------ INPUTS ------------------------------------------------------
    @ComponentInput(
            name = "VOLUME ID",
            description = "The volume object" +
                "<br>TYPE: java.lang.String" +
                "<br>TYPE: org.seasr.datatypes.BasicDataTypes.Strings"
    )
    protected static final String INPUT_VOLUMEIDS = "VOLUME ID";

    //------------------------------ OUTPUTS -----------------------------------------------------
    @ComponentOutput(
            name = Names.PORT_TOKEN_COUNTS,
            description = "The word count pulled from Solr server" +
                "<br>TYPE: org.seasr.datatypes.BasicDataTypes.IntegersMap"
    )
    protected static final String WORD_COUNT = Names.PORT_TOKEN_COUNTS;

    //------------------------------ PROPERTIES --------------------------------------------------
    @ComponentProperty(
            description = "The connection timeout in milliseconds " +
                    "(amount of time to wait for a connection to be established before giving up; 0 = wait forever)",
            name = Names.PROP_CONNECTION_TIMEOUT,
            defaultValue = "0"
    )
    protected static final String PROP_CONNECTION_TIMEOUT = Names.PROP_CONNECTION_TIMEOUT;

    @ComponentProperty(
            description = "The read timeout in milliseconds " +
                    "(amount of time to wait for a read operation to complete before giving up; 0 = wait forever)",
            name = Names.PROP_READ_TIMEOUT,
            defaultValue = "0"
    )
    protected static final String PROP_READ_TIMEOUT = Names.PROP_READ_TIMEOUT;

    @ComponentProperty(
            description = "The endpoint for HTRC Solr",
            name = "HTRC Solr Endpoint",
            defaultValue = "http://coffeetree.cs.indiana.edu:9994/solr"
    )
    protected static final String SOLR_EPR = "HTRC Solr Endpoint";

    @ComponentProperty(
            description = "The delimiter for volume",
            name = "Delimiter for volume",
            defaultValue = "|"
    )
    protected static final String DELIMITER = "Delimiter for volume";

    @ComponentProperty(
            defaultValue = "100",
            description = "This property sets the maximum number of tokens to be output.",
            name = Names.PROP_N_TOP_TOKENS
    )
    protected static final String PROP_UPPER_LIMIT = Names.PROP_N_TOP_TOKENS;
    //--------------------------------------------------------------------------------------------

    protected int connectionTimeout;
    protected int readTimeout;
    protected String solrEPR;
    protected String delimiter;
    protected int upperLimit;
    //--------------------------------------------------------------------------------------------

    protected HTRCSolrClient client = null;

    @Override
    public void initializeCallBack(ComponentContextProperties ccp) throws Exception {
        super.initializeCallBack(ccp);

        connectionTimeout = Integer.parseInt(getPropertyOrDieTrying(PROP_CONNECTION_TIMEOUT, ccp));
        readTimeout = Integer.parseInt(getPropertyOrDieTrying(PROP_READ_TIMEOUT, ccp));
        solrEPR = getPropertyOrDieTrying(SOLR_EPR, ccp);
        delimiter = getPropertyOrDieTrying(DELIMITER, ccp);
        String limitStr = getPropertyOrDieTrying(PROP_UPPER_LIMIT, true, false, ccp);
        upperLimit = limitStr.length() > 0 ? Integer.parseInt(limitStr) : Integer.MAX_VALUE;

        client = new HTRCSolrClient(solrEPR);
    }

    @Override
    public void executeCallBack(ComponentContext cc) throws Exception {
        // get volume id list
        String[] volumes = DataTypeParser.parseAsString(
                cc.getDataComponentFromInput(INPUT_VOLUMEIDS));

        // check solr query format ??

        // parse to map
        Map<String, Integer> wordCount =
                client.getWordCountInCountOrderFromSolrFilter(Arrays.asList(volumes), false);
//    	System.out.println("get word count from solr!!!!!");


        // filter
        Map<String, Integer> result = new HashMap<String, Integer>();
        int count = 0;
        Iterator<String> iterator = wordCount.keySet().iterator();
        while (count < upperLimit && iterator.hasNext()) {
            String word = iterator.next();
            Integer freq = wordCount.get(word);
            result.put(word, freq);
            count++;
        }


        StreamInitiator si = new StreamInitiator(streamId);
        componentContext.pushDataComponentToOutput(WORD_COUNT, si);

        cc.pushDataComponentToOutput(WORD_COUNT,
                BasicDataTypesTools.mapToIntegerMap(result, false));
//    	System.out.println("!!!! push word count size " + result.size());

        StreamTerminator st = new StreamTerminator(streamId);
        componentContext.pushDataComponentToOutput(WORD_COUNT, st);
    }

    @Override
    public void disposeCallBack(ComponentContextProperties ccp) throws Exception {
        client = null;
    }

    @Override
    public boolean isAccumulator() {
        return false;
    }
}
