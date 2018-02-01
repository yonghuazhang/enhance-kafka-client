package org.apache.kafka.clients.enhance;

import org.apache.kafka.common.Node;

import java.util.Collection;

public class ClusterDescription {

    private final Collection<Node> nodes;
    private final Node controller;
    private final String clusterId;

    public ClusterDescription(Collection<Node> nodes, Node controller, String clusterId) {
        this.nodes = nodes;
        this.controller = controller;
        this.clusterId = clusterId;
    }

    public Collection<Node> getNodes() {
        return nodes;
    }

    public Node getController() {
        return controller;
    }

    public String getClusterId() {
        return clusterId;
    }
}
