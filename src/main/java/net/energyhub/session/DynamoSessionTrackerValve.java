/***********************************************************************************************************************
 *
 * Dynamo Tomcat Sessions
 * ==========================================
 *
 * Copyright (C) 2012 by Dawson Systems Ltd (http://www.dawsonsystems.com)
 * Copyright (C) 2013 by EnergyHub Inc. (http://www.energyhub.com)
 *
 ***********************************************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package net.energyhub.session;

import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;


public class DynamoSessionTrackerValve extends ValveBase {
    final private static Logger log = Logger.getLogger(DynamoSessionTrackerValve.class.getName());

    private DynamoManager manager;

    protected String ignoreUri = "";
    protected String ignoreHeader = "";
    private Pattern ignoreUriPattern;
    private Pattern ignoreHeaderPattern;

    public void setDynamoManager(DynamoManager manager) {
        this.manager = manager;
    }

    @Override
    public void initInternal() {
        if (!getIgnoreUri().isEmpty()) {
            log.info("Setting URI ignore regex to: " + getIgnoreUri());
            ignoreUriPattern = Pattern.compile(getIgnoreUri());
        }
        if (!getIgnoreHeader().isEmpty()) {
            log.info("Setting header ignore regex to: " + getIgnoreHeader());
            ignoreHeaderPattern = Pattern.compile(getIgnoreHeader());
        }
    }

    @Override
    public void invoke(Request request, Response response) throws IOException, ServletException {
        try {
            getNext().invoke(request, response);
        } finally {
            if (!isIgnorable(request)) {
                manager.afterRequest();
            }
        }
    }

    protected boolean isIgnorable(Request request) {
        if (ignoreUriPattern != null) {
            if (ignoreUriPattern.matcher(request.getRequestURI()).matches()) {
                if (log.isLoggable(Level.FINE)) {
                    log.fine("Session manager will ignore this session based on uri: " + request.getRequestURI());
                }
                return true;
            }
        }
        if (ignoreHeaderPattern != null) {
            for (Enumeration headers = request.getHeaderNames(); headers.hasMoreElements(); ) {
                String header = headers.nextElement().toString();
                if (ignoreHeaderPattern.matcher(header).matches()) {
                    if (log.isLoggable(Level.FINE)) {
                        log.fine("Session manager will ignore this session based on header: " + header);
                    }
                    return true;
                }

            }
        }
        return false;
    }

    public String getIgnoreUri() {
        return ignoreUri;
    }

    public String getIgnoreHeader() {
        return ignoreHeader;
    }

    public void setIgnoreUri(String ignoreUri) {
        this.ignoreUri = ignoreUri;
    }

    public void setIgnoreHeader(String ignoreHeader) {
        this.ignoreHeader = ignoreHeader;
    }
}
