/**
 * Copyright 2012 Lennart Koopmann <lennart@socketfeed.com>
 *
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package org.graylog2.forwarders.forwarders;

import org.apache.log4j.Logger;
import org.graylog2.forwarders.MessageForwarderIF;
import org.graylog2.messagehandlers.gelf.GELFMessage;

/**
 * OpenTsdbForwarder.java: Mar 16, 2011 4:54:25 PM
 *
 * Forwards metrics extracted from a message to an OpenTSDB server.
 *
 * @author Lennart Koopmann <lennart@socketfeed.com>
 */
public class OpenTsdbForwarder extends TCPForwarder implements MessageForwarderIF {

    private static final Logger LOG = Logger.getLogger(OpenTsdbForwarder.class);

    private boolean succeeded = false;

    public OpenTsdbForwarder(String host, int port) throws MessageForwarderConfigurationException {
        this.setHost(host);
        this.setPort(port);
    }

    @Override
    public boolean forward(GELFMessage message) throws MessageForwarderConfigurationException {
        if (this.host.isEmpty() || this.port <= 0) {
            throw new MessageForwarderConfigurationException("Host is empty or port is invalid.");
        }

        this.succeeded = this.send("put lol.wut 1 100 foo=bar");

        return this.succeeded;
    }

    /**
     * Indicates if the last forward has succeeded.
     *
     * @return
     */
    @Override
    public boolean succeeded() {
        return this.succeeded;
    }

}