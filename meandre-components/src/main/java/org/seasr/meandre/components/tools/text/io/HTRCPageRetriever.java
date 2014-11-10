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
        creator = "Jiaan Zeng",
        description = "Retrieves pages of text from the HTRC Data API service",
        name = "HTRC Page Retriever",
        rights = Licenses.UofINCSA,
        tags = "#INPUT, text, htrc, page",
        dependency = {
                "protobuf-java-2.2.0.jar", "dataapi-client-0.6.2.jar", "pairtree-1.1.1.jar",
                "commons-logging-1.1.1.jar", "jettison-1.2.jar", "slf4j-api-1.6.1.jar",
                "oauth2-client-0.22.1358727-wso2v2.jar", "oauth2-common-0.22.1358727-wso2v2.jar" },
        baseURL = "meandre://seasr.org/components/htrc/"
)
public class HTRCPageRetriever extends AbstractStreamingExecutableComponent {

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
            description = "The text content pulled from the Data API server for each page of each volume" +
                "<br>TYPE: org.seasr.datatypes.BasicDataTypes.Strings"
    )
    protected static final String OUT_TEXT = Names.PORT_TEXT;

    @ComponentOutput(
            name = "volume_id",
            description = "The volume id for each page of each volume" +
                "<br>TYPE: org.seasr.datatypes.BasicDataTypes.Strings"
    )
    protected static final String OUT_VOLUMEID = "volume_id";

    @ComponentOutput(
            name = "page_id",
            description = "The page id for each page of each volume" +
                "<br>TYPE: org.seasr.datatypes.BasicDataTypes.Strings"
    )
    protected static final String OUT_PAGEID = "page_id";

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
            description = "The delimiter for the volume list",
            name = "delimiter",
            defaultValue = "|"
    )
    protected static final String PROP_DELIMITER = "delimiter";

    @ComponentProperty(
            name = Names.PROP_WRAP_STREAM,
            description = "Enable streaming for the output?",
            defaultValue = "true"
    )
    protected static final String PROP_WRAP_STREAM = Names.PROP_WRAP_STREAM;

    @ComponentProperty(
            name = "stream_per_volume",
            description = "Create a stream for each volume in the input (containing the set of pages for that volume)?",
            defaultValue = "true"
    )
    protected static final String PROP_STREAM_PER_VOLUME = "stream_per_volume";

    //--------------------------------------------------------------------------------------------


    protected int connectionTimeout;
    protected int readTimeout;
    protected String dataAPIEPR;
    protected String delimiter;

    protected String token;

    protected boolean selfsign;
    protected boolean wrapStream;
    protected boolean streamPerVolume;

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
        streamPerVolume = Boolean.parseBoolean(getPropertyOrDieTrying(PROP_STREAM_PER_VOLUME, ccp));

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
        String volumes = DataTypeParser.parseAsString(cc.getDataComponentFromInput(IN_VOLUMEIDS))[0];

        // convert into real list of volume ids
        String[] volumeIDs = volumes.split(Pattern.quote(delimiter));

        // construct the query path for the DataAPI request
        String queryStr = HTRCDataClient.ids2URL(Arrays.asList(volumeIDs), delimiter);

        console.finer(String.format("ids2URL: returned '%s'", queryStr));

        // start a global stream, if necessary
        if (wrapStream && !streamPerVolume)
            pushStreamMarker(new StreamInitiator(streamId));

        String prevVolumeId = null;
        int pageId = 1;

        Iterable<Entry<String, String>> pages = client.getID2Page(queryStr);
        if (pages != null) {
            for (Entry<String, String> page : pages) {
                final String volumeId = page.getKey();
                final String pageContent = page.getValue();

                if (volumeId == null || pageContent == null) {
                    String msg = "";
                    if (volumeId == null) msg += "volumeId";
                    if (pageContent == null) {
                        if (volumeId == null) msg += " and ";
                        msg += "pageContent";
                        if (volumeId != null)
                            msg += " for volume id " + volumeId;
                    }
                    console.severe(String.format("getID2Page: Returned NULL %s! Ignoring page...", msg));
                    continue;
                }

                if (volumeId != prevVolumeId) {
                    // check whether to output a start or end stream marker,
                    // if streaming is one and streamPerVolume is set
                    if (wrapStream && streamPerVolume) {
                        if (prevVolumeId != null)
                            pushStreamMarker(new StreamTerminator(streamId));

                        pushStreamMarker(new StreamInitiator(streamId));
                    }

                    prevVolumeId = volumeId;
                    pageId = 1;
                } else
                    pageId++;

                console.fine(String.format("Pushing out vol_id: %s  page_id: %d", volumeId, pageId));

                cc.pushDataComponentToOutput(OUT_TEXT, BasicDataTypesTools.stringToStrings(pageContent));
                cc.pushDataComponentToOutput(OUT_VOLUMEID, BasicDataTypesTools.stringToStrings(volumeId));
                cc.pushDataComponentToOutput(OUT_PAGEID, BasicDataTypesTools.stringToStrings(Integer.toString(pageId)));
            }

            // send an end stream marker for the last volume
            if (wrapStream && streamPerVolume) {
                if (prevVolumeId != null)
                    pushStreamMarker(new StreamTerminator(streamId));
            }
        } else
            console.warning("getID2Page: Returned NULL - possible communication error with the DataAPI service");

        // end the global stream, if necessary
        if (wrapStream && !streamPerVolume)
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
        componentContext.pushDataComponentToOutput(OUT_PAGEID, sd);
    }
}
