package com.dashradar.privatesendanalysis;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

public class PsGraph extends DirectedMultigraph<String, DefaultEdge> {
    
    public PsGraph() {
        super(DefaultEdge.class);
    }   
    
    public Optional<String> rootNode() {
        return this.vertexSet().stream().filter(vertex -> this.inDegreeOf(vertex) == 0).findFirst();
    }    
    
    public List<String> leafNodes() {
        return this.vertexSet().stream().filter(vertex -> this.outDegreeOf(vertex) == 0).collect(Collectors.toList());
    }
    
}
