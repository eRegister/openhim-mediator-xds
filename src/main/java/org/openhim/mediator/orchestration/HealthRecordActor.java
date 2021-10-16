/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.openhim.mediator.orchestration;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.openhim.mediator.datatypes.AssigningAuthority;
import org.openhim.mediator.datatypes.Identifier;
import org.openhim.mediator.denormalization.PIXRequestActor;
import org.openhim.mediator.denormalization.RegistryResponseError;
import org.openhim.mediator.engine.MediatorConfig;
import org.openhim.mediator.engine.messages.*;
import org.openhim.mediator.messages.*;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;


public class HealthRecordActor extends UntypedActor {
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private MediatorConfig config;

    protected ActorRef resolvePatientIDActor;

    private ActorRef requestHandler;
    private String xForwardedFor;
    private String messageBuffer;
    private Identifier patientId;
    private Identifier resolvedPatientId;
    private String messageID;
    private boolean isStoredQuery;
    private String requestParams;
    private String path;
    private MediatorHTTPRequest originalRequest;

    private static final String customAttributes = "custom:(display,concept,person,obsDatetime,location,groupMembers,value)";
    private static final String erDelimiter = "^^^";

    public HealthRecordActor(MediatorConfig config) {
        this.config = config;

        resolvePatientIDActor = getContext().actorOf(Props.create(PIXRequestActor.class, config), "pix-denormalization");
    }

    private void lookupEnterpriseIdentifier() {
        String enterpriseIdentifierAuthority = config.getProperty("client.requestedAssigningAuthority");
        String enterpriseIdentifierAuthorityId = config.getProperty("client.requestedAssigningAuthorityId");
        AssigningAuthority authority = new AssigningAuthority(enterpriseIdentifierAuthority, enterpriseIdentifierAuthorityId);
        ResolvePatientIdentifier msg = new ResolvePatientIdentifier(requestHandler, getSelf(), patientId, authority);
        resolvePatientIDActor.tell(msg, getSelf());
    }

    private void forwardToRegistry() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/soap+xml");

        String scheme;
        Integer port;
        if (config.getProperty("xds.registry.secure").equals("true")) {
            scheme = "https";
            port = Integer.parseInt(config.getProperty("xds.registry.securePort"));
        } else {
            scheme = "http";
            port = Integer.parseInt(config.getProperty("xds.b.registry.port"));
        }

        MediatorHTTPRequest request = new MediatorHTTPRequest(
                requestHandler, getSelf(), "XDS.b Registry", "POST", scheme,
                config.getProperty("xds.registry.host"), port, config.getProperty("xds.registry.path"),
                messageBuffer, headers, Collections.<String, String>emptyMap()
        );

        ActorSelection httpConnector = getContext().actorSelection(config.userPathFor("http-connector"));
        httpConnector.tell(request, getSelf());
    }

    private void getSHRPatientUuid(Identifier patientId) {
        log.info("Requesting patient observations from shared health record!");
        ActorSelection httpConnector = getContext().actorSelection(config.userPathFor("http-connector"));

        Charset charset = Charset.forName("US-ASCII");
        String contentType = originalRequest.getHeaders().get("Content-Type");
        Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json");
        String auth = config.getProperty("xds.repository.api.user") + ":" + config.getProperty("xds.repository.api.password");
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(charset));
        String authHeader = "Basic " + new String(encodedAuth, charset);
        headers.put("Authorization", authHeader);

        Map<String, String> params = new HashMap<>();
        params.put("identifier", patientId.getIdentifier());

        String scheme;
        Integer port;
        if (config.getProperty("xds.repository.secure").equals("true")) {
            scheme = "https";
            port = Integer.parseInt(config.getProperty("xds.repository.securePort"));
        } else {
            scheme = "http";
            port = Integer.parseInt(config.getProperty("xds.repository.port"));
        }

        MediatorHTTPRequest request = new MediatorHTTPRequest(
                originalRequest.getRespondTo(), getSelf(), "XDS Repository Patient UUID Request", "GET", scheme,
                config.getProperty("xds.repository.host"), port, config.getProperty("xds.repository.patient.path"),
                messageBuffer, headers, params
        );

        httpConnector.tell(request, getSelf());
    }

    private void getSHRPatientObservations(String uuid) {
        log.info("Requesting patient observations from shared health record!");
        ActorSelection httpConnector = getContext().actorSelection(config.userPathFor("http-connector"));

        Charset charset = Charset.forName("US-ASCII");
        String contentType = originalRequest.getHeaders().get("Content-Type");
        Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json");
        String auth = config.getProperty("xds.repository.api.user") + ":" + config.getProperty("xds.repository.api.password");
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(charset));
        String authHeader = "Basic " + new String(encodedAuth, charset);
        headers.put("Authorization", authHeader);

        Map<String, String> params = new HashMap<>();
        params.put("patient", uuid);
        params.put("v", customAttributes);

        String scheme;
        Integer port;
        if (config.getProperty("xds.repository.secure").equals("true")) {
            scheme = "https";
            port = Integer.parseInt(config.getProperty("xds.repository.securePort"));
        } else {
            scheme = "http";
            port = Integer.parseInt(config.getProperty("xds.repository.port"));
        }

        MediatorHTTPRequest request = new MediatorHTTPRequest(
                originalRequest.getRespondTo(), getSelf(), "XDS Repository Rest", "GET", scheme,
                config.getProperty("xds.repository.host"), port, config.getProperty("xds.repository.rest.path"),
                messageBuffer, headers, params
        );

        httpConnector.tell(request, getSelf());
    }

    private void finalizeResponse(MediatorHTTPResponse response) {
        requestHandler.tell(response.toFinishRequest(), getSelf());
    }

    private void sendAuditMessage(ATNAAudit.TYPE type, boolean outcome) {
        try {
            ATNAAudit audit = new ATNAAudit(type);
            audit.setMessage(messageBuffer);

            audit.setParticipantIdentifiers(Collections.singletonList(patientId));
            audit.setUniqueId("NotParsed");
            audit.setOutcome(outcome);
            audit.setSourceIP(xForwardedFor);

            getContext().actorSelection(config.userPathFor("atna-auditing")).tell(audit, getSelf());
        } catch (Exception ex) {
            //quiet you!
        }
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof MediatorHTTPRequest) {
            originalRequest = (MediatorHTTPRequest) msg;
            String localPatientId = ((MediatorHTTPRequest) msg).getParams().get("patient").toString();
            String identifierDomain = ((MediatorHTTPRequest) msg).getParams().get("identifierDomain").toString();
            path = ((MediatorHTTPRequest) msg).getPath();
            requestParams = ((MediatorHTTPRequest) msg).getParams().get("v").toString();

            String patientID_CX = localPatientId + erDelimiter + identifierDomain + "&" + identifierDomain + "&PI";
            patientId = new Identifier(patientID_CX);

            requestHandler = ((MediatorHTTPRequest) msg).getRequestHandler();
            lookupEnterpriseIdentifier();

        } else if (msg instanceof ResolvePatientIdentifierResponse) {
            log.info("Received resolved patient identifier.");
            getSHRPatientUuid(((ResolvePatientIdentifierResponse) msg).getIdentifier());

        } else if (msg instanceof MediatorHTTPResponse) {
            if(((MediatorHTTPResponse) msg).getOriginalRequest().getOrchestration()
                    == "XDS Repository Patient UUID Request") {
                log.info("Receiving patient uuid from SHR ...");
                ObjectMapper mapper = new ObjectMapper();
                String jsonResponse = ((MediatorHTTPResponse) msg).getBody();
                JsonNode node;

                if(StringUtils.isNotBlank(jsonResponse)) {
                    node = mapper.readTree(jsonResponse);
                    String uuidResponse = node.get("results").get(0).get("uuid").asText().toString();
                    getSHRPatientObservations(uuidResponse);
                }

            } else {
                log.info("Returning observations to the client...");
                finalizeResponse((MediatorHTTPResponse) msg);

            }
        } else {
            unhandled(msg);
        }
    }
}
