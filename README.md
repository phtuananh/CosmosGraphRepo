# Graph
Quick building graph on Azure Cosmos Graph database based on gremlin query

Publish Nuget package: msbuild /t:pack /p:Configuration=Release

Usage:

public class Node1 : GraphNode, IUId
{
    public string UId { get; set; } 
    public string Name { get; set; }
    public DateTime Date { get; set; }
}
public class Node2 : GraphNode, IUId
{
    public string UId { get; set; } 
    public string Name { get; set; }
    public DateTime Date { get; set; }
}
public class EdgeSample : GraphEdge, IUid
{
    public string UId { get; set; }
    public DateTime Date { get; set; }
}
// POST
var repo = new GraphRepo(endpoint, authKey, databaseName, collectionName, throughput);
var node1 = new Node1{UId="1"};
var node2 = new Node2{UId="2"};
var edge = new EdgeSample{UId="3"};

var savedNode1 = await repo.AddNodeIfNotExistAsync(node1);
var savedNode2 = await repo.AddNodeIfNotExistAsync(node2);

// Create Edge using node's id
var savedEdge = await  repo.AddEdgeIfNotExistAsync(node1.Id, edge, node2.Id)
var savedEdge = await  repo.AddEdgeAsync(node1.Id, edge, node2.Id)

// Create Edge using node's UId
var savedEdge = await repo.AddEdgeByUIdIfNotExistAsync<EdgeSample, Node1, Node2>(node1.UId, edge, node2.UId)
var savedEdge = await repo.AddEdgeByUIdAsync<EdgeSample, Node1, Node2>(node1.UId, edge, node2.UId)

// GET
var n1 = await repo.GetNodeByIdAsync<Node1>(savedNode1.Id);
var n2 = await repo.GetNodeByUIdAsync<Node1>(node1.UId);

var e1 = await repo.GetEdgeByIdAsync<Node1>(savedEdge.Id);
var e2 = await repo.GetEdgeByUIdAsync<Node1>(edge.UId);

// Drop
await repo.DropAsyn();
// Execute
var gremlinQuery = "g.V().count()";
await repo.ExecuteAsync(gremlinQuery);
