package org.seasr.meandre.components.tools.text.io;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.meandre.annotations.Component;
import org.meandre.annotations.Component.Licenses;
import org.meandre.annotations.ComponentInput;
import org.meandre.annotations.ComponentOutput;
import org.meandre.annotations.ComponentProperty;
import org.meandre.core.ComponentContext;
import org.meandre.core.ComponentContextException;
import org.meandre.core.ComponentContextProperties;
import org.meandre.core.system.components.ext.StreamDelimiter;
import org.meandre.core.system.components.ext.StreamInitiator;
import org.meandre.core.system.components.ext.StreamTerminator;
import org.seasr.datatypes.core.BasicDataTypesTools;
import org.seasr.datatypes.core.DataTypeParser;
import org.seasr.datatypes.core.Names;
import org.seasr.meandre.components.abstracts.AbstractStreamingExecutableComponent;

import edu.indiana.d2i.htrc.clients.dataapi.HTRCDataClient;

@Component(
        creator = "Boris Capitanu",
        description = "Retrieves volume text from the HTRC Data API service",
        name = "HTRC Volume Retriever",
        rights = Licenses.UofINCSA,
        tags = "#INPUT, text, htrc, volume",
        dependency = {
                "protobuf-java-2.2.0.jar", "dataapi-client-0.6.2.jar", "pairtree-1.1.1.jar",
                "commons-logging-1.1.1.jar", "jettison-1.2.jar", "slf4j-api-1.6.1.jar",
                "oauth2-client-0.22.1358727-wso2v2.jar", "oauth2-common-0.22.1358727-wso2v2.jar" },
        baseURL = "meandre://seasr.org/components/htrc/"
)
public class HTRCVolumeRetriever extends AbstractStreamingExecutableComponent {

    //------------------------------ INPUTS ------------------------------------------------------

    @ComponentInput(
            name = "volume_id_list",
            description = "The list of volume ids. " +
                    "The ids in the list should be delimited by the delimiter specified in the 'delimiter' property." +
                    "<br>TYPE: java.lang.String" +
                    "<br>TYPE: org.seasr.datatypes.BasicDataTypes.Strings"
    )
    protected static final String IN_VOLUMEIDS = "volume_id_list";

    //------------------------------ OUTPUTS -----------------------------------------------------

    @ComponentOutput(
            name = Names.PORT_TEXT,
            description = "The volume text" +
                "<br>TYPE: org.seasr.datatypes.BasicDataTypes.Strings"
    )
    protected static final String OUT_TEXT = Names.PORT_TEXT;

    @ComponentOutput(
            name = "volume_id",
            description = "The volume id" +
                "<br>TYPE: org.seasr.datatypes.BasicDataTypes.Strings"
    )
    protected static final String OUT_VOLUMEID = "volume_id";

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
            description = "The service URL endpoint for HTRC Data API",
            name = "data_api_url",
            defaultValue = "https://silvermaple.pti.indiana.edu:25443/data-api/"
    )
    protected static final String PROP_DATA_API_EPR = "data_api_url";

    @ComponentProperty(
            description = "The delimiter for the volume list",
            name = "delimiter",
            defaultValue = "|"
    )
    protected static final String PROP_DELIMITER = "delimiter";

    @ComponentProperty(
            description = "The OAuth2 authentication token.",
            name = "auth_token",
            defaultValue = ""
    )
    protected static final String PROP_AUTH_TOKEN = "auth_token";

    @ComponentProperty(
            description = "Use self sign or not",
            name = "auth_selfsign",
            defaultValue = "true"
    )
    protected static final String PROP_AUTH_SELFSIGN = "auth_selfsign";

    @ComponentProperty(
            name = Names.PROP_WRAP_STREAM,
            description = "Enable streaming for the output?",
            defaultValue = "true"
    )
    protected static final String PROP_WRAP_STREAM = Names.PROP_WRAP_STREAM;

    //--------------------------------------------------------------------------------------------


    protected int connectionTimeout;
    protected int readTimeout;
    protected String dataAPIEPR;
    protected String delimiter;

    protected String token;

    protected boolean selfsign;
    protected boolean wrapStream;

    protected HTRCDataClient client;


    //--------------------------------------------------------------------------------------------

    @Override
    public void initializeCallBack(ComponentContextProperties ccp) throws Exception {
        super.initializeCallBack(ccp);

        connectionTimeout = Integer.parseInt(getPropertyOrDieTrying(PROP_CONNECTION_TIMEOUT, ccp));
        readTimeout = Integer.parseInt(getPropertyOrDieTrying(PROP_READ_TIMEOUT, ccp));
        dataAPIEPR = getPropertyOrDieTrying(PROP_DATA_API_EPR, ccp);
        delimiter = getPropertyOrDieTrying(PROP_DELIMITER, ccp);
        selfsign = Boolean.parseBoolean(getPropertyOrDieTrying(PROP_AUTH_SELFSIGN, ccp));
        wrapStream = Boolean.parseBoolean(getPropertyOrDieTrying(PROP_WRAP_STREAM, ccp));

        token = getPropertyOrDieTrying(PROP_AUTH_TOKEN, true, false, ccp);

        boolean useAuthentication = !token.isEmpty();
        if (!useAuthentication)
            console.fine("No authentication information provided. Performing unauthenticated requests.");

        HTRCDataClient.Builder builder = new HTRCDataClient.Builder(dataAPIEPR)
            .connectionTimeout(connectionTimeout).readTimeout(readTimeout);

        if (useAuthentication)
            builder.selfsigned(selfsign).token(token);

        client = builder.build();
    }

    @Override
    public void executeCallBack(ComponentContext cc) throws Exception {
        // retrieve the delimited volume id list from input
        String volumeList = DataTypeParser.parseAsString(cc.getDataComponentFromInput(IN_VOLUMEIDS))[0];

        // convert into real list of volume ids
        String[] volumeIDs = volumeList.split(Pattern.quote(delimiter));

        // construct the query path for the DataAPI request
        String queryStr = HTRCDataClient.ids2URL(Arrays.asList(volumeIDs), delimiter);

        // start a global stream, if necessary
        if (wrapStream)
            pushStreamMarker(new StreamInitiator(streamId));

        Iterable<Entry<String, String>> volumes = client.getID2Content(queryStr);
        for (Entry<String, String> volume : volumes) {
            String volumeId = volume.getKey();
            String volumeText = volume.getValue();

            if (volumeId == null || volumeText == null) {
                console.warning("One of volumeId or volumeText is NULL - this should not happen!");
                continue;
            }

            console.finer(String.format("Pushing: vol_id: %s (volume text length: %d)", volumeId, volumeText.length()));

            cc.pushDataComponentToOutput(OUT_TEXT, BasicDataTypesTools.stringToStrings(volumeText));
            cc.pushDataComponentToOutput(OUT_VOLUMEID, BasicDataTypesTools.stringToStrings(volumeId));
        }

        // end the global stream, if necessary
        if (wrapStream)
            pushStreamMarker(new StreamTerminator(streamId));
    }

    @Override
    public void disposeCallBack(ComponentContextProperties ccp) throws Exception {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    //--------------------------------------------------------------------------------------------

    @Override
    public boolean isAccumulator() {
        return false;
    }

    //--------------------------------------------------------------------------------------------

    private void pushStreamMarker(StreamDelimiter sd) throws ComponentContextException {
        componentContext.pushDataComponentToOutput(OUT_TEXT, sd);
        componentContext.pushDataComponentToOutput(OUT_VOLUMEID, sd);
    }
}
