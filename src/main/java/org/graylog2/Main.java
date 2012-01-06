/**
 * Copyright 2010, 2011, 2012 Lennart Koopmann <lennart@socketfeed.com>, Kay Roepke
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

package org.graylog2;

import com.beust.jcommander.JCommander;
import com.github.joschi.jadconfig.JadConfig;
import com.github.joschi.jadconfig.RepositoryException;
import com.github.joschi.jadconfig.ValidationException;
import com.github.joschi.jadconfig.repositories.PropertiesRepository;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.graylog2.database.MongoConnection;
import org.graylog2.forwarders.forwarders.LogglyForwarder;
import org.graylog2.indexer.Indexer;
import org.graylog2.messagequeue.MessageQueueFlusher;
import org.graylog2.periodical.HostCounterCacheWriterThread;
import org.graylog2.periodical.MessageCountWriterThread;
import org.graylog2.periodical.MessageRetentionThread;
import org.graylog2.periodical.ServerValueWriterThread;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Main class of Graylog2.
 *
 * @author Lennart Koopmann <lennart@socketfeed.com>
 */
public final class Main {

    private static final Logger LOG = Logger.getLogger(Main.class);
    private static final String GRAYLOG2_VERSION = "0.9.7-dev";

    private static final int SCHEDULED_THREADS_POOL_SIZE = 7;

    public static Configuration configuration = null;
    public static ScheduledExecutorService scheduler = null;

    private Main() {
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        CommandLineArguments commandLineArguments = new CommandLineArguments();
        JCommander jCommander = new JCommander(commandLineArguments, args);
        jCommander.setProgramName("graylog2");

        if (commandLineArguments.isShowHelp()) {
            jCommander.usage();
            System.exit(0);
        }

        if (commandLineArguments.isShowVersion()) {
            System.out.println("Graylog2 Server " + GRAYLOG2_VERSION);
            System.out.println("JRE: " + Tools.getSystemInformation());
            System.exit(0);
        }

        // Are we in debug mode?
        if (commandLineArguments.isDebug()) {
            LOG.info("Running in Debug mode");
            Logger.getRootLogger().setLevel(Level.ALL);
            Logger.getLogger(Main.class.getPackage().getName()).setLevel(Level.ALL);
        }

        LOG.info("Graylog2 starting up. (JRE: " + Tools.getSystemInformation() + ")");

        String configFile = commandLineArguments.getConfigFile();
        LOG.info("Using config file: " + configFile);

        configuration = new Configuration();
        JadConfig jadConfig = new JadConfig(new PropertiesRepository(configFile), configuration);

        LOG.info("Loading configuration");
        try {
            jadConfig.process();
        } catch (RepositoryException e) {
            LOG.fatal("Couldn't load configuration file " + configFile, e);
            System.exit(1);
        } catch (ValidationException e) {
            LOG.fatal("Invalid configuration", e);
            System.exit(1);
        }

        // If we only want to check our configuration, we can gracefully exit here
        if (commandLineArguments.isConfigTest()) {
            System.exit(0);
        }

        // XXX ELASTIC: put in own method
        // Check if the index exists. Create it if not.
        try {
            if (Indexer.indexExists()) {
                LOG.info("Index exists. Not creating it.");
            } else {
                LOG.info("Index does not exist! Trying to create it ...");
                if (Indexer.createIndex()) {
                    LOG.info("Successfully created index.");
                } else {
                    LOG.fatal("Could not create Index. Terminating.");
                    System.exit(1);
                }
            }
        } catch (IOException e) {
            LOG.fatal("IOException while trying to check Index. Make sure that your ElasticSearch server is running.", e);
            System.exit(1);
        }

        savePidFile(commandLineArguments.getPidFile());

        // Statically set timeout for LogglyForwarder.
        // TODO: This is a code smell and needs to be fixed.
        LogglyForwarder.setTimeout(configuration.getForwarderLogglyTimeout());

        initializeMongoConnection(configuration);

        // Register outputs.

        // Register filters/(post-)triggers.

        // Start inputs.


        // PERIODICALS
        scheduler = Executors.newScheduledThreadPool(SCHEDULED_THREADS_POOL_SIZE);
        initializeHostCounterCache(scheduler);
        initializeMessageCounters(scheduler);
        writeInitialServerValues(configuration);
        initializeServerValueWriter(scheduler);
        if (commandLineArguments.performRetention()) {
            initializeMessageRetentionThread(scheduler);
        } else {
            LOG.info("Not initializing retention time cleanup thread because --no-retention was passed.");
        }
        // //

        // Add a shutdown hook that tries to flush the message queue.
	Runtime.getRuntime().addShutdownHook(new MessageQueueFlusher());

        LOG.info("Graylog2 up and running.");
    }

    private static void initializeHostCounterCache(ScheduledExecutorService scheduler) {

        scheduler.scheduleAtFixedRate(new HostCounterCacheWriterThread(), HostCounterCacheWriterThread.INITIAL_DELAY, HostCounterCacheWriterThread.PERIOD, TimeUnit.SECONDS);

        LOG.info("Host count cache is up.");
    }

    private static void initializeMessageCounters(ScheduledExecutorService scheduler) {

        scheduler.scheduleAtFixedRate(new MessageCountWriterThread(), MessageCountWriterThread.INITIAL_DELAY, MessageCountWriterThread.PERIOD, TimeUnit.SECONDS);

        LOG.info("Message counters initialized.");
    }

    private static void initializeServerValueWriter(ScheduledExecutorService scheduler) {

        scheduler.scheduleAtFixedRate(new ServerValueWriterThread(), ServerValueWriterThread.INITIAL_DELAY, ServerValueWriterThread.PERIOD, TimeUnit.SECONDS);

        LOG.info("Server value writer up.");
    }

    private static void initializeMessageRetentionThread(ScheduledExecutorService scheduler) {
        // Schedule first run. This is NOT at fixed rate. Thread will itself schedule next run with current frequency setting from database.
        scheduler.schedule(new MessageRetentionThread(),0,TimeUnit.SECONDS);

        LOG.info("Retention time management active.");
    }

    private static void initializeMongoConnection(Configuration configuration) {
        try {
            MongoConnection.getInstance().connect(
                    configuration.getMongoUser(),
                    configuration.getMongoPassword(),
                    configuration.getMongoHost(),
                    configuration.getMongoDatabase(),
                    configuration.getMongoPort(),
                    configuration.isMongoUseAuth(),
                    configuration.getMongoMaxConnections(),
                    configuration.getMongoThreadsAllowedToBlockMultiplier(),
                    configuration.getMongoReplicaSet(),
                    configuration.getMessagesCollectionSize()
            );
        } catch (Exception e) {
            LOG.fatal("Could not create MongoDB connection: " + e.getMessage(), e);
            System.exit(1); // Exit with error.
        }
    }

    private static void savePidFile(String pidFile) {

        String pid = Tools.getPID();
        Writer pidFileWriter = null;

        try {
            if (pid == null || pid.isEmpty() || pid.equals("unknown")) {
                throw new Exception("Could not determine PID.");
            }

            pidFileWriter = new FileWriter(pidFile);
            IOUtils.write(pid, pidFileWriter);
        } catch (Exception e) {
            LOG.fatal("Could not write PID file: " + e.getMessage(), e);
            System.exit(1);
        } finally {
            IOUtils.closeQuietly(pidFileWriter);
        }
    }

    public static void writeInitialServerValues(Configuration configuration) {
        ServerValue.setStartupTime(Tools.getUTCTimestamp());
        ServerValue.setPID(Integer.parseInt(Tools.getPID()));
        ServerValue.setJREInfo(Tools.getSystemInformation());
        ServerValue.setGraylog2Version(GRAYLOG2_VERSION);
        ServerValue.setAvailableProcessors(HostSystem.getAvailableProcessors());
        ServerValue.setLocalHostname(Tools.getLocalHostname());
        ServerValue.writeMessageQueueMaximumSize(configuration.getMessageQueueMaximumSize());
        ServerValue.writeMessageQueueBatchSize(configuration.getMessageQueueBatchSize());
        ServerValue.writeMessageQueuePollFrequency(configuration.getMessageQueuePollFrequency());
    }
}
