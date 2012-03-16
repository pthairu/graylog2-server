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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import org.apache.log4j.Logger;

/**
 * TCPForwarder.java: Mar 16, 2011 4:59:28 PM
 *
 * Base class to forward messages via TCP.
 *
 * @author Lennart Koopmann <lennart@socketfeed.com>
 */
public class TCPForwarder {

    private static final Logger LOG = Logger.getLogger(TCPForwarder.class);

    protected String host = null;
    protected int port = 0;

    protected TCPForwarder() { /* Nothing. This is not meant to be instantiated. */ }

    protected boolean send(String what) {
        try {
            Socket clientSocket = new Socket(this.getHost(), this.getPort());
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            outToServer.writeBytes(what);
            clientSocket.close();
        } catch (IOException e) {
            return false;
        }

        return true;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port the port to set
     */
    public void setPort(int port) {
        this.port = port;
    }

}