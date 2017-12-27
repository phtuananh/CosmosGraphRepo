using System.Collections.Generic;
using System.Linq;

namespace CosmosGraphRepo
{
    /// <summary>
    /// UId is internal id
    /// </summary>
    public interface IUid
    {
        string UId { get; set; }
    }
    /// <summary>
    /// Vertex or Edge
    /// </summary>
    public interface IGraphElement
    {
        string Id { get; set; }
        string Label { get; }
    }
    /// <summary>
    /// Vertex/Node having multi properties of same name
    /// </summary>
    public interface IGraphNode : IGraphElement
    {
        /// <summary>
        /// Add new value of a property name
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        void AddProperty(string name, object value);
        /// <summary>
        /// Get all values of a property name
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="name"></param>
        /// <returns></returns>
        IEnumerable<T> GetPropertyValues<T>(string name);
        /// <summary>
        /// Get All properties
        /// </summary>
        /// <returns></returns>
        IEnumerable<KeyValuePair<string, object>> GetProperties();
    }
    /// <summary>
    /// Edge
    /// </summary>
    public interface IGraphEdge : IGraphElement
    {

    }
    /// <summary>
    /// GraphElement
    /// </summary>
    public abstract class GraphElement : IGraphElement
    {
        // Do not set it if not having good raison
        public string Id { get; set; }
        // By default equal Type.Name
        public string Label { get; } 
        public static string IdName = "Id";

        protected GraphElement()
        {
            Label = GetType().Name;
        }
    }
    /// <summary>
    /// GraphEdge
    /// </summary>
    public abstract class GraphEdge : GraphElement, IGraphEdge
    {

    }
    /// <summary>
    /// GraphNode
    /// </summary>
    public abstract class GraphNode : GraphElement, IGraphNode
    {
        private readonly List<KeyValuePair<string, object>> _properties = new List<KeyValuePair<string, object>>();

        public void AddProperty(string name, object value)
        {
            _properties.Add(new KeyValuePair<string, object>(name, value));
        }

        public IEnumerable<T> GetPropertyValues<T>(string name)
        {
            return _properties.Where(x => x.Key == name).Select(x => x.Value).Cast<T>();
        }

        public IEnumerable<KeyValuePair<string, object>> GetProperties() => _properties;

    }
    /// <summary>
    /// Helper
    /// </summary>
    public static class GraphHelper
    {
        public static string GetLabel<TGraphElement>() where TGraphElement : IGraphElement, new ()
        {
            return new TGraphElement().Label;
        }
    }
}
