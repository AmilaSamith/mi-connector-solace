/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.integration.connector.connection;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.connection.ConnectionHandler;

/**
 * Tracks active transactional connections by transaction id, with an auto-rollback
 * watchdog so a forgotten commit/rollback doesn't leak the transacted session.
 */
public final class SolaceTransactionRegistry {

    private static final Log log = LogFactory.getLog(SolaceTransactionRegistry.class);

    private static final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private static ScheduledExecutorService watchdog = newWatchdog();

    private SolaceTransactionRegistry() {}

    private static ScheduledExecutorService newWatchdog() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "solace-tx-watchdog");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Returns the watchdog scheduler, recreating it if a prior {@link #shutdown()} terminated
     * it. Guards against scheduling on a dead executor (RejectedExecutionException) when the
     * connector is used again after a teardown — e.g. a same-classloader redeploy, or
     * {@code destroy()} firing while the connector is still in use elsewhere.
     */
    private static synchronized ScheduledExecutorService watchdog() {
        if (watchdog.isShutdown()) {
            watchdog = newWatchdog();
        }
        return watchdog;
    }

    public static String register(SolaceConnection connection, String connectionName, long timeoutMillis) {
        String txId = UUID.randomUUID().toString();
        Entry entry = new Entry(connection, connectionName);
        entry.timeoutFuture = watchdog().schedule(() -> autoRollback(txId),
                timeoutMillis, TimeUnit.MILLISECONDS);
        entries.put(txId, entry);
        log.info("TransactionRegistry: registered txId=" + txId + " (connectionName=" + connectionName
                + ", connectionId=" + connection.getConnectionId() + ", timeoutMillis=" + timeoutMillis
                + ", activeCount=" + entries.size() + ")");
        return txId;
    }

    public static SolaceConnection get(String txId) {
        Entry e = entries.get(txId);
        if (e == null) {
            log.debug("TransactionRegistry: lookup miss for txId=" + txId
                    + " (activeCount=" + entries.size() + ")");
            return null;
        }
        return e.connection;
    }

    public static Entry unregister(String txId) {
        Entry e = entries.remove(txId);
        if (e != null && e.timeoutFuture != null) {
            e.timeoutFuture.cancel(false);
            log.debug("TransactionRegistry: unregistered txId=" + txId
                    + " (connectionName=" + e.connectionName + ", activeCount=" + entries.size() + ")");
            return e;
        }
        log.debug("TransactionRegistry: unregister miss for txId=" + txId
                + " (activeCount=" + entries.size() + ")");
        return null;
    }

    /**
     * Shuts down the watchdog executor and clears any remaining entries. Called on connector
     * teardown ({@code SolaceConfigConnector.destroy}) so the daemon thread doesn't outlive the
     * connector's classloader — a lingering thread would pin the classloader and leak it (and
     * the thread) across redeploys. Outstanding transacted sessions are released by the
     * connection-pool shutdown that follows; here we just stop the scheduler and drop entries.
     */
    public static synchronized void shutdown() {
        watchdog.shutdownNow();
        int remaining = entries.size();
        if (remaining > 0) {
            log.warn("TransactionRegistry: shutting down with " + remaining + " active transaction(s)"
                    + " still registered; their connections will be closed by the pool shutdown.");
        }
        entries.clear();
        log.info("TransactionRegistry: watchdog shut down.");
    }

    private static void autoRollback(String txId) {
        Entry e = entries.remove(txId);
        if (e == null) return;
        log.warn("Transaction " + txId + " timed out — auto-rolling back (connectionName="
                + e.connectionName + ", connectionId=" + e.connection.getConnectionId() + ")");
        try {
            e.connection.rollbackTransaction();
            log.info("TransactionRegistry: auto-rollback completed for txId=" + txId);
        } catch (Exception ex) {
            log.error("Auto-rollback failed for transaction " + txId, ex);
        } finally {
            try {
                ConnectionHandler.getConnectionHandler().returnConnection(
                        SolaceConstants.CONNECTOR_NAME, e.connectionName, e.connection);
                log.debug("TransactionRegistry: returned connection to pool after auto-rollback for txId="
                        + txId);
            } catch (Exception ex) {
                log.error("Failed to return Solace connection to pool after auto-rollback for tx "
                        + txId, ex);
            }
        }
    }

    public static class Entry {
        public final SolaceConnection connection;
        public final String connectionName;
        ScheduledFuture<?> timeoutFuture;
        Entry(SolaceConnection c, String n) {
            this.connection = c;
            this.connectionName = n;
        }
    }
}
