# mi-connector-solace
The Solace connector enables WSO2 Integrator: MI to publish messages and interact with a Solace broker. It supports reliable, asynchronous communication, allowing you to send messages to queues or topics, and manage message acknowledgments.

## Prerequisites

The Solace JCSMP client (`sol-jcsmp`) is **not bundled** with this connector. Solace JCSMP is not Apache-2.0 licensed, so it cannot be redistributed or repackaged. You must install it once into the MI server's shared library directory, `<MI_HOME>/lib`, before deploying.

Installing it in `<MI_HOME>/lib` (rather than bundling it inside the connector) is required so that this connector and the [Solace inbound endpoint](https://github.com/wso2-extensions/mi-inbound-solace) share a **single** copy of the client. A single shared copy:

- avoids a Netty initialization clash — `java.lang.IllegalArgumentException: 'TOTAL_SOCKET_BYTES_SENT' is already in use` — that occurs when two copies of the JCSMP Netty transport are loaded in the same server (e.g. the connector and the inbound deployed together, or a hot-redeploy), and
- lets the inbound endpoint hand its received message to this connector's `acknowledge` / `nack` operations (both modules must resolve `com.solacesystems.jcsmp.BytesXMLMessage` from the same classloader).

### Install the required JARs

Place the following into `<MI_HOME>/lib` and restart the server:

| Artifact | Version |
|---|---|
| `com.solacesystems:sol-jcsmp` | `10.30.1` |
| `org.apache.servicemix.bundles:org.apache.servicemix.bundles.jzlib` | `1.1.3_2` |

Netty does **not** need to be added — the JCSMP client uses the Netty already shipped with MI (MI 4.6.0 bundles `netty-all 4.2.12`, which matches `sol-jcsmp 10.30.x`).

You can fetch both JARs from Maven into `lib` with:

```bash
mvn dependency:copy -Dartifact=com.solacesystems:sol-jcsmp:10.30.1 -DoutputDirectory="$MI_HOME/lib"
mvn dependency:copy -Dartifact=org.apache.servicemix.bundles:org.apache.servicemix.bundles.jzlib:1.1.3_2 -DoutputDirectory="$MI_HOME/lib"
```

Then restart WSO2 MI.
