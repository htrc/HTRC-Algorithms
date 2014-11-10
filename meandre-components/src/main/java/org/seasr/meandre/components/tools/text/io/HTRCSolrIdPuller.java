package org.seasr.meandre.components.tools.text.io;

import java.util.List;

import org.meandre.annotations.Component;
import org.meandre.annotations.Component.Licenses;
import org.meandre.annotations.ComponentInput;
import org.meandre.annotations.ComponentOutput;
import org.meandre.annotations.ComponentProperty;
import org.meandre.core.ComponentContext;
import org.meandre.core.ComponentContextProperties;
import org.seasr.datatypes.core.BasicDataTypesTools;
import org.seasr.datatypes.core.DataTypeParser;
import org.seasr.datatypes.core.Names;
import org.seasr.meandre.components.abstracts.AbstractExecutableComponent;

import edu.indiana.d2i.htrc.clients.solr.HTRCSolrClient;

@Component(
        creator = "Jiaan Zeng",
        description = "Pull text from Data API service. ",
        name = "HTRC Solr Puller",
        rights = Licenses.UofINCSA,
        tags = "text",
        dependency = {"protobuf-java-2.2.0.jar", "dataapi-client-0.6.2.jar", "pairtree-1.1.1.jar"},
        baseURL = "meandre://seasr.org/components/foundry/"
)
public class HTRCSolrIdPuller extends AbstractExecutableComponent {
    //------------------------------ INPUTS ------------------------------------------------------
    @ComponentInput(
            name = "solr_query",
            description = "The query parameter for SOLR" +
                "<br>TYPE: java.lang.String" +
                "<br>TYPE: org.seasr.datatypes.BasicDataTypes.Strings"
    )
    protected static final String INPUT_PARAM = "solr_query";

    //------------------------------ OUTPUTS -----------------------------------------------------
    @ComponentOutput(
            name = "volume_id_list",
            description = "The list of volume ids pulled from Solr server" +
                "<br>TYPE: org.seasr.datatypes.BasicDataTypes.Strings"
    )
    protected static final String OUT_VOLUMEIDS = "volume_id_list";

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
            name = "solr_endpoint",
            defaultValue = "http://coffeetree.cs.indiana.edu:9994/solr"
    )
    protected static final String PROP_SOLR_EPR = "solr_endpoint";

    @ComponentProperty(
            description = "The delimiter for volume",
            name = "delimiter",
            defaultValue = "|"
    )
    protected static final String PROP_DELIMITER = "delimiter";
    //--------------------------------------------------------------------------------------------

    protected int connectionTimeout;
    protected int readTimeout;
    protected String solrEPR;
    protected String delimiter;
    //--------------------------------------------------------------------------------------------

    protected HTRCSolrClient client = null;

    @Override
    public void initializeCallBack(ComponentContextProperties ccp) throws Exception {
        connectionTimeout = Integer.parseInt(getPropertyOrDieTrying(PROP_CONNECTION_TIMEOUT, ccp));
        readTimeout = Integer.parseInt(getPropertyOrDieTrying(PROP_READ_TIMEOUT, ccp));
        solrEPR = getPropertyOrDieTrying(PROP_SOLR_EPR, ccp);
        delimiter = getPropertyOrDieTrying(PROP_DELIMITER, ccp);

        client = new HTRCSolrClient(solrEPR);
    }

    @Override
    public void executeCallBack(ComponentContext cc) throws Exception {
        String[] args = DataTypeParser.parseAsString(
                cc.getDataComponentFromInput(INPUT_PARAM));

        // check solr query format ??

        // parse to map
        List<String> volumeIDs = client.getVolumeIDs(args[0]);

        cc.pushDataComponentToOutput(OUT_VOLUMEIDS,
                BasicDataTypesTools.stringToStrings(volumeIDs.toArray(new String[0])));
    }

    @Override
    public void disposeCallBack(ComponentContextProperties ccp) throws Exception {
        client = null;
    }
}
