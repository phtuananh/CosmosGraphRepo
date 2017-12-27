using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Graphs;
using Microsoft.Azure.Graphs.Elements;

namespace CosmosGraphRepo
{
    /// <summary>
    /// Graph Repo
    /// </summary>
    public abstract class GraphRepo : DocumentCollection
    {
        public DocumentClient Client { get; set; }
        public DocumentCollection Collection { get; set; }

        protected GraphRepo(string endpoint, string authKey, string databaseName, string collectionName, int throughput=400)
        {
            if (string.IsNullOrEmpty(endpoint)) throw new ArgumentNullException(nameof(endpoint));
            if (string.IsNullOrEmpty(authKey)) throw new ArgumentNullException(nameof(authKey));
            if (string.IsNullOrEmpty(databaseName)) throw new ArgumentNullException(nameof(databaseName));
            if (string.IsNullOrEmpty(collectionName)) throw new ArgumentNullException(nameof(collectionName));
            (Client, Collection) = InitDbAsync(endpoint, authKey, databaseName, collectionName, throughput).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        private static async Task<(DocumentClient client, DocumentCollection collection)> InitDbAsync(string endpoint, string authKey, string databaseName, string collectionName, int throughput)
        {
            var client = new DocumentClient(new Uri(endpoint), authKey, new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp });
            await client.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseName }, new RequestOptions());
            var collection = await client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(databaseName),
                new DocumentCollection { Id = collectionName },
                new RequestOptions { OfferThroughput = throughput });
            return (client, collection);
        }

        public void Deconstruct(out DocumentClient client, out DocumentCollection collection)
        {
            client = Client;
            collection = Collection;
        }

        #region Collection
        public async Task DropAsync()
        {
            await ExecuteAsync("g.E().drop()");
            await ExecuteAsync("g.V().drop()");
        }
        public async Task<IEnumerable<dynamic>> ExecuteAsync(string query)
        {
            var gremlinQuery = Client.CreateGremlinQuery<dynamic>(Collection, query);
            var results = new List<dynamic>();
            while (gremlinQuery.HasMoreResults)
            {
                foreach (dynamic result in await gremlinQuery.ExecuteNextAsync())
                {
                    results.Add(result);
                }
            }
            return results;
        }
        #endregion

        #region Edges
        // By NodeId
        public async Task<TEdge> AddEdgeAsync<TEdge>(string fromNodeId, TEdge edge, string toNodeId)
            where TEdge : class, IGraphEdge, new()
        {
            var query = new StringBuilder();
            query.Append($"g.V('{fromNodeId}').addE('{edge.Label}').to(g.V('{toNodeId}'))");
            if (!string.IsNullOrEmpty(edge.Id)) query.Append($".property('id', '{edge.Id}')");

            var edgeProps = GetNodeProperties<TEdge>();
            foreach (var prop in edgeProps.Values)
            {
                var propName = prop.Name;
                var propValue = prop.GetValue(edge);
                if (propName == GraphElement.IdName) continue;
                query.Append($".property('{propName}','{propValue}')");
            }
            //
            var savedEdges = await GetEdges<TEdge>(query.ToString());
            return savedEdges.FirstOrDefault();
        }

        public Task<TEdge> AddEdgeAsync<TEdge>(IGraphNode fromNode, TEdge edge, IGraphNode toNode)
            where TEdge : class, IGraphEdge, new()
        {
            return AddEdgeAsync<TEdge>(fromNode.Id, edge, toNode.Id);
        }

        public async Task<TEdge> AddEdgeIfNotExistAsync<TEdge>(string fromNodeId, TEdge edge, string toNodeId)
            where TEdge : class, IGraphEdge, IUid, new()
        {
            var existEdge = await GetEdgeByUIdAsync<TEdge>(edge.UId);
            if (existEdge != null) return existEdge;
            var newEdge = await AddEdgeAsync<TEdge>(fromNodeId, edge, toNodeId);
            return newEdge;
        }
        // By Node UId
        public Task<TEdge> AddEdgeByUIdAsync<TEdge, TNode>(string fromNodeUId, TEdge edge, string toNodeUId)
            where TEdge : class, IGraphEdge, new()
            where TNode : IGraphNode, new()
        {
            return AddEdgeByUIdAsync<TEdge, TNode, TNode>(fromNodeUId, edge, toNodeUId);
        }

        public async Task<TEdge> AddEdgeByUIdIfNotExistAsync<TEdge, TNode>(string fromNodeUId, TEdge edge, string toNodeUId)
            where TEdge : class, IGraphEdge, IUid, new()
            where TNode : IGraphNode, new()
        {
            var existEdge = await GetEdgeByUIdAsync<TEdge>(edge.UId);
            if (existEdge != null) return existEdge;
            var newEdge = await AddEdgeByUIdAsync<TEdge, TNode>(fromNodeUId, edge, toNodeUId);
            return newEdge;
        }
        public async Task<TEdge> AddEdgeByUIdIfNotExistAsync<TEdge, TNodeFrom, TNodeTo>(string fromNodeUId, TEdge edge, string toNodeUId)
            where TEdge : class, IGraphEdge, IUid, new()
            where TNodeFrom : IGraphNode, new()
            where TNodeTo : IGraphNode, new()
        {
            var existEdge = await GetEdgeByUIdAsync<TEdge>(edge.UId);
            if (existEdge != null) return existEdge;
            var newEdge = await AddEdgeByUIdAsync<TEdge, TNodeFrom, TNodeTo>(fromNodeUId, edge, toNodeUId);
            return newEdge;
        }
        public async Task<TEdge> AddEdgeByUIdAsync<TEdge, TNodeFrom, TNodeTo>(string fromNodeUId, TEdge edge, string toNodeUId)
            where TEdge : class, IGraphEdge, new()
            where TNodeFrom : IGraphNode, new()
            where TNodeTo : IGraphNode, new()
        {
            var query = new StringBuilder();
            var fromNodeLabel = new TNodeFrom().Label;
            var toNodeLabel = new TNodeTo().Label;
            query.Append($"g.V().hasLabel('{fromNodeLabel}').has('UId','{fromNodeUId}').addE('{edge.Label}').to(g.V().hasLabel('{toNodeLabel}').has('UId','{toNodeUId}'))");
            if (!string.IsNullOrEmpty(edge.Id)) query.Append($".property('id', '{edge.Id}')");

            var edgeProps = GetNodeProperties<TEdge>();
            foreach (var prop in edgeProps.Values)
            {
                var propName = prop.Name;
                var propValue = prop.GetValue(edge);
                if (propName == GraphElement.IdName) continue;
                query.Append($".property('{propName}','{propValue}')");
            }
            //
            var savedEdges = await GetEdges<TEdge>(query.ToString());
            return savedEdges.FirstOrDefault();
        }

        public async Task<TEdge> GetEdgeByIdAsync<TEdge>(string edgeId)
            where TEdge : class, IGraphEdge, new()
        {
            var edges = await GetEdges<TEdge>($"g.E('{edgeId}')");
            return edges.FirstOrDefault();
        }

        public async Task<TEdge> GetEdgeByUIdAsync<TEdge>(string edgeUId)
            where TEdge : class, IGraphEdge, IUid, new()
        {
            var edgeLabel = new TEdge().Label;
            var edges = await GetEdges<TEdge>($"g.E().hasLabel('{edgeLabel}').has('UId','{edgeUId}')");
            return edges.FirstOrDefault();
        }

        public async Task<List<TEdge>> GetEdges<TEdge>(string query)
            where TEdge : class, IGraphEdge, new()
        {
            var nodes = new List<TEdge>();
            var vertexQuery = Client.CreateGremlinQuery<Edge>(Collection, query);
            while (vertexQuery.HasMoreResults)
            {
                foreach (var edge in await vertexQuery.ExecuteNextAsync<Edge>())
                {
                    var node = DeserializeEdge<TEdge>(edge);
                    if (node != null) nodes.Add(node);
                }
            }
            return nodes;
        }

        private static TEdge DeserializeEdge<TEdge>(Edge edge) where TEdge : class, IGraphEdge, new()
        {
            if (edge == null || edge.Label != GraphHelper.GetLabel<TEdge>()) return null;

            var edgePropsDic = edge.GetProperties()
                .Where(x => !string.IsNullOrEmpty(x?.Key))
                .Select(x => new KeyValuePair<string, object>(x.Key, x.Value))
                .ToList();

            var nodePropsDic = GetNodeProperties<TEdge>();

            var node = new TEdge { Id = edge.Id.ToString() };
            foreach (var edgePropKeyPair in edgePropsDic)
            {
                var edgePropName = edgePropKeyPair.Key;
                var edgePropValue = edgePropKeyPair.Value;
                if (edgePropName == GraphElement.IdName) continue;

                if (nodePropsDic.TryGetValue(edgePropName, out PropertyInfo nodeProperty))
                {
                    nodeProperty.SetValue(node, Convert.ChangeType(edgePropValue, nodeProperty.PropertyType), null);
                }
            }
            return node;
        }
        #endregion

        #region Nodes

        public async Task<TNode> AddNodeIfNotExistAsync<TNode>(TNode node)
            where TNode : class, IGraphNode, IUid, new()
        {
            var existNode = await GetNodeByUIdAsync<TNode>(node.UId);
            if (existNode != null)
            {
                node.Id = existNode.Id;
                return existNode;
            }
            var newNode = await AddNodeAsync(node);
            return newNode;
        }
        public async Task<TNode> AddNodeAsync<TNode>(TNode node)
            where TNode : class, IGraphNode, new()
        {
            if (node == null) throw new ArgumentNullException(nameof(node));
            var query = new StringBuilder($"g.addV('{node.Label}')");
            if (!string.IsNullOrEmpty(node.Id)) query.Append($".property('id', '{node.Id}')");

            var props = new List<KeyValuePair<string, object>>();

            var nodeProps = GetNodeProperties<TNode>();
            foreach (var prop in nodeProps.Values)
            {
                var propName = prop.Name;
                var propValue = prop.GetValue(node);
                if (propName == GraphElement.IdName) continue;
                props.Add(new KeyValuePair<string, object>(propName, propValue));
            }
            var otherNodeProps = node.GetProperties();
            foreach (var otherNodeProp in otherNodeProps)
            {
                props.Add(new KeyValuePair<string, object>(otherNodeProp.Key, otherNodeProp.Value));
            }
            foreach (var pair in props)
            {
                if (pair.Value == null) continue;
                query.Append($".property('{pair.Key}','{pair.Value}')");
            }
            //
            var savedNodes = await GetNodesAsync<TNode>(query.ToString());
            return savedNodes.FirstOrDefault();
        }

        public async Task<TNode> GetNodeByIdAsync<TNode>(string nodeId)
            where TNode : class, IGraphNode, new()
        {
            var nodes = await GetNodesAsync<TNode>($"g.V('{nodeId}')");
            return nodes.FirstOrDefault();
        }
        public async Task<TNode> GetNodeByUIdAsync<TNode>(string uId)
            where TNode : class, IGraphNode, IUid, new()
        {
            var nodeLabel = GraphHelper.GetLabel<TNode>();
            var nodes = await GetNodesAsync<TNode>($"g.V().hasLabel('{nodeLabel}').has('UId','{uId}')");
            return nodes.FirstOrDefault();
        }

        public async Task<List<TNode>> GetNodesAsync<TNode>(string query)
            where TNode : class, IGraphNode, new()
        {
            var nodes = new List<TNode>();
            var vertexQuery = Client.CreateGremlinQuery<Vertex>(Collection, query);

            while (vertexQuery.HasMoreResults)
            {
                foreach (var vertex in await vertexQuery.ExecuteNextAsync<Vertex>())
                {
                    var node = DeserializeNode<TNode>(vertex);
                    if (node != null) nodes.Add(node);
                }
            }
            return nodes;
        }

        private static TNode DeserializeNode<TNode>(Vertex vertex) where TNode : class, IGraphNode, new()
        {
            if (vertex == null || vertex.Label != GraphHelper.GetLabel<TNode>()) return null;

            var vertexPropsDic = vertex.GetVertexProperties()
                .Where(x => !string.IsNullOrEmpty(x?.Key))
                .Select(x => new KeyValuePair<string, object>(x.Key, x.Value))
                .ToList();

            var nodePropsDic = GetNodeProperties<TNode>();

            var node = new TNode { Id = vertex.Id.ToString() };
            var settedProperty = new HashSet<string>();
            foreach (var vertexPropKeyPair in vertexPropsDic)
            {
                var vertexPropName = vertexPropKeyPair.Key;
                var vertexPropValue = vertexPropKeyPair.Value;
                if (vertexPropName == GraphElement.IdName) continue;
                if (nodePropsDic.TryGetValue(vertexPropName, out PropertyInfo nodeProperty))
                {
                    node.AddProperty(vertexPropName, vertexPropValue);
                    if (!settedProperty.Contains(vertexPropName))
                    {
                        nodeProperty.SetValue(node, Convert.ChangeType(vertexPropValue, nodeProperty.PropertyType), null);
                        settedProperty.Add(vertexPropName);
                    }
                }
            }
            return node;
        }

        private static Dictionary<string, PropertyInfo> GetNodeProperties<TNode>() where TNode : IGraphElement
        {
            var nodeType = typeof(TNode).Name;
            lock (NodeProperties)
            {
                if (NodeProperties.TryGetValue(nodeType, out Dictionary<string, PropertyInfo> result)) return result;
                var nodePropsDic = typeof(TNode)
                    .GetProperties()
                    .Where(x => x.CanRead && x.CanWrite)
                    .Where(x => x.MemberType == MemberTypes.Property && x.PropertyType.IsPublic)
                    .Where(x => !x.PropertyType.IsSubclassOf(typeof(IEnumerable)))
                    .Where(x => x.GetSetMethod() != null && (x.GetSetMethod().Attributes & MethodAttributes.Static) == 0)
                    .ToDictionary(x => x.Name, x => x);
                NodeProperties[nodeType] = nodePropsDic;
                return nodePropsDic;
            }
        }

        private static readonly ConcurrentDictionary<string, Dictionary<string, PropertyInfo>> NodeProperties = new ConcurrentDictionary<string, Dictionary<string, PropertyInfo>>();
        #endregion
    }
}
