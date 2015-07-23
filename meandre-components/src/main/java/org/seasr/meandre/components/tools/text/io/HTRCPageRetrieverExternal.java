package org.seasr.meandre.components.tools.text.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.meandre.annotations.Component;
import org.meandre.annotations.Component.Licenses;
import org.meandre.annotations.ComponentInput;
import org.meandre.annotations.ComponentOutput;
import org.meandre.annotations.ComponentProperty;
import org.meandre.core.ComponentContext;
import org.meandre.core.ComponentContextException;
import org.meandre.core.ComponentContextProperties;
import org.meandre.core.ComponentExecutionException;
import org.meandre.core.system.components.ext.StreamDelimiter;
import org.meandre.core.system.components.ext.StreamInitiator;
import org.meandre.core.system.components.ext.StreamTerminator;
import org.seasr.datatypes.core.BasicDataTypes.Strings;
import org.seasr.datatypes.core.BasicDataTypes.StringsArray;
import org.seasr.datatypes.core.BasicDataTypesTools;
import org.seasr.datatypes.core.Names;
import org.seasr.meandre.components.abstracts.AbstractStreamingExecutableComponent;
import org.seasr.meandre.support.components.tuples.SimpleTuple;
import org.seasr.meandre.support.components.tuples.SimpleTuplePeer;

import edu.indiana.d2i.htrc.clients.dataapi.HTRCDataClient;

@Component(
        creator = "Jiaan Zeng",
        description = "Retrieves pages of text from the HTRC Data API service",
        name = "HTRC Page Retriever External",
        rights = Licenses.UofINCSA,
        tags = "#INPUT, text, htrc, page",
        dependency = {
                "protobuf-java-2.2.0.jar", "dataapi-client-0.6.jar", "pairtree-1.1.1.jar",
                "commons-logging-1.1.1.jar", "jettison-1.2.jar", "slf4j-api-1.7.6.jar",
                "oauth2-client-0.22.1358727-wso2v2.jar", "oauth2-common-0.22.1358727-wso2v2.jar" },
        baseURL = "meandre://seasr.org/components/htrc/"
)
public class HTRCPageRetrieverExternal extends AbstractStreamingExecutableComponent {
	
	protected static final String HTRC_VOLUME_ID = "htrc.volume.id";
	protected static final String HTRC_VOLUME_EPR = "htrc.volume.epr";
	protected static final String DELIMITER = "|";
    
    //------------------------------ INPUTS ------------------------------------------------------

    @ComponentInput(
            name = Names.PORT_TUPLES,
            description = "The set of tuples" +
                "<br>TYPE: org.seasr.datatypes.BasicDataTypes.StringsArray"
    )
    protected static final String IN_TUPLES = Names.PORT_TUPLES;

    @ComponentInput(
            name = Names.PORT_META_TUPLE,
            description = "The meta data for tuples" +
                "<br>TYPE: org.seasr.datatypes.BasicDataTypes.Strings"
    )
    protected static final String IN_META_TUPLE = Names.PORT_META_TUPLE;

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
            description = "The service endpoint URL for the default HTRC Data API to use, if unspecified with the input tuples",
            name = "data_api_url",
            defaultValue = "https://silvermaple.pti.indiana.edu:25443/data-api"
    )
    protected static final String PROP_DATA_API_EPR = "data_api_url";
    
    @ComponentProperty(
            description = "The maximum number of volumes to ask for in a single request. " +
                          "(requests for a set of volumes larger than this number from a single EPR will be broken down into multiple requests, each of a size <= this number; 0 = no max limit)",
            name = "max_volumes_per_request",
            defaultValue = "0"
    )
    protected static final String PROP_MAX_VOLS_PER_REQ = "max_volumes_per_request";

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
    protected int maxVolsPerReq;

    protected String token;

    protected boolean selfsign;
    protected boolean wrapStream;
    protected boolean streamPerVolume;

    boolean useAuthentication;


    //--------------------------------------------------------------------------------------------

    @Override
    public void initializeCallBack(ComponentContextProperties ccp) throws Exception {
        super.initializeCallBack(ccp);

        connectionTimeout = Integer.parseInt(getPropertyOrDieTrying(PROP_CONNECTION_TIMEOUT, ccp));
        readTimeout = Integer.parseInt(getPropertyOrDieTrying(PROP_READ_TIMEOUT, ccp));
        dataAPIEPR = getPropertyOrDieTrying(PROP_DATA_API_EPR, ccp);
        maxVolsPerReq = Integer.parseInt(getPropertyOrDieTrying(PROP_MAX_VOLS_PER_REQ, ccp));
        selfsign = Boolean.parseBoolean(getPropertyOrDieTrying(PROP_AUTH_SELFSIGN, ccp));
        wrapStream = Boolean.parseBoolean(getPropertyOrDieTrying(PROP_WRAP_STREAM, ccp));
        streamPerVolume = Boolean.parseBoolean(getPropertyOrDieTrying(PROP_STREAM_PER_VOLUME, ccp));

        token = getPropertyOrDieTrying(PROP_AUTH_TOKEN, true, false, ccp);

        useAuthentication = !token.isEmpty();
        if (!useAuthentication)
            console.fine("No authentication information provided. Performing unauthenticated requests.");
    }

    @Override
    public void executeCallBack(ComponentContext cc) throws Exception {
    	// get the mapping from dataAPI EPR to list of volumes served by that EPR
        Map<String, List<String>> volMap = getEprVolumesMap(cc);
     
        // start a global stream, if necessary
        if (wrapStream && !streamPerVolume)
            pushStreamMarker(new StreamInitiator(streamId));
        
        console.finer(String.format("wrapStream: %s streamPerVolume: %s", wrapStream, streamPerVolume));
        
        for (Entry<String, List<String>> entry : volMap.entrySet()) {
        	String epr = entry.getKey();
        	List<String> volumeIDsForEpr = entry.getValue();
        	console.finer(String.format("endpoint: %s with %s",epr,volumeIDsForEpr));
        	
        	for (List<String> volumeIDs : partition(volumeIDsForEpr, maxVolsPerReq)) {	
	        	HTRCDataClient.Builder builder = new HTRCDataClient.Builder(epr)
	        		.connectionTimeout(connectionTimeout).readTimeout(readTimeout);
	
				if (useAuthentication)
				   builder.selfsigned(selfsign).token(token);
				
	        	HTRCDataClient client = builder.build();
	        	try {
	        		// construct the query path for the DataAPI request
	        		String queryStr = HTRCDataClient.ids2URL(volumeIDs, DELIMITER);
	
	        		console.finer(String.format("ids2URL returned: '%s'", queryStr));
	
	        		String prevVolumeId = null;
	        		int pageId = 1;
	
	        		Iterable<Entry<String, String>> pages = client.getID2Page(queryStr);
	        		if (pages != null) {
	        			int volCount = 0;
	        			int pageCount = 0;
	        			for (Entry<String, String> page : pages) {
	        				final String volumeId = page.getKey();
	        				final String pageContent = page.getValue();
	        				pageCount++;
	
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
	
	        					volCount++;
	        					prevVolumeId = volumeId;
	        					pageId = 1;
	        				} else
	        					pageId++;
	
	        				console.finer(String.format("Pushing out vol_id: %s  page_id: %d", volumeId, pageId));
	
	        				cc.pushDataComponentToOutput(OUT_TEXT, BasicDataTypesTools.stringToStrings(pageContent));
	        				cc.pushDataComponentToOutput(OUT_VOLUMEID, BasicDataTypesTools.stringToStrings(volumeId));
	        				cc.pushDataComponentToOutput(OUT_PAGEID, BasicDataTypesTools.stringToStrings(Integer.toString(pageId)));
	        			}
	        			
	        			console.fine(String.format("%s: Pushed out %d volumes with a total of %d pages", epr, volCount, pageCount));
	
	        			// send an end stream marker for the last volume
	        			if (wrapStream && streamPerVolume) {
	        				if (prevVolumeId != null)
	        					pushStreamMarker(new StreamTerminator(streamId));
	        			}
	        		} else
	        			console.warning("getID2Page: Returned NULL - possible communication error with the DataAPI service");
	        	}
	        	finally {
	        		client.close();
	        	}
        	}
        }
        
        // end the global stream, if necessary
        if (wrapStream && !streamPerVolume)
            pushStreamMarker(new StreamTerminator(streamId));
    }

	@Override
    public void disposeCallBack(ComponentContextProperties ccp) throws Exception {
    }

    //--------------------------------------------------------------------------------------------

    @Override
    public boolean isAccumulator() {
        return false;
    }

    //--------------------------------------------------------------------------------------------
	
	private List<List<String>> partition(List<String> list, int count) {
		int numPartitions = count > 0 ? (int)Math.ceil((double)list.size() / count) : 1;
		List<List<String>> partitions = new ArrayList<List<String>>(numPartitions);
		
		if (count == 0)
			partitions.add(list);
		else {
			for (int i = 0, iMax = list.size(); i < iMax; i += count)
				partitions.add(list.subList(i, Math.min(i+count, iMax)));
		}
		
		return partitions;
	}
	
	private Map<String, List<String>> getEprVolumesMap(ComponentContext cc)
			throws ComponentContextException, ComponentExecutionException {
		Strings inputMeta = (Strings) cc.getDataComponentFromInput(IN_META_TUPLE);
	    SimpleTuplePeer tuplePeer = new SimpleTuplePeer(inputMeta);
	    SimpleTuple tuple = tuplePeer.createTuple();
	
	    StringsArray input = (StringsArray) cc.getDataComponentFromInput(IN_TUPLES);
	    Strings[] in = BasicDataTypesTools.stringsArrayToJavaArray(input);
	
	    int volCount = in.length;
	    int idIdx = tuplePeer.getIndexForFieldName(HTRC_VOLUME_ID);
	    int eprIdx = tuplePeer.getIndexForFieldName(HTRC_VOLUME_EPR);
	    
	    console.fine(String.format("volCount: %,d   idIdx: %d   eprIdx: %d", volCount, idIdx, eprIdx));
	    
	    if (idIdx < 0)
	    	throw new ComponentExecutionException("Missing " + HTRC_VOLUME_ID + " from input tuples");
	    
	    if (eprIdx < 0)
	    	console.warning("Missing " + HTRC_VOLUME_EPR + " from input tuples - assuming default");
		
	    Map<String, List<String>> volMap = new HashMap<String, List<String>>();
	    for (int i = 0; i < volCount; i++) {
	    	tuple.setValues(in[i]);
	    	String volId = tuple.getValue(idIdx).trim();
	    	String volEpr = (eprIdx < 0 ? dataAPIEPR : tuple.getValue(eprIdx)).trim();
	    	if (!volEpr.endsWith("/")) volEpr += "/";
	    	List<String> vols = volMap.get(volEpr);
	    	if (vols == null) {
	    		vols = new ArrayList<String>();
	    		volMap.put(volEpr, vols);
	    	}
	    	vols.add(volId);
	    }
	    
		return volMap;
	}

	private void pushStreamMarker(StreamDelimiter sd) throws ComponentContextException {
        componentContext.pushDataComponentToOutput(OUT_TEXT, sd);
        componentContext.pushDataComponentToOutput(OUT_VOLUMEID, sd);
        componentContext.pushDataComponentToOutput(OUT_PAGEID, sd);
    }
}
