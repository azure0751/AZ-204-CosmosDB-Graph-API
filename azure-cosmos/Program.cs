using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Structure.IO.GraphSON;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Axio.Stuff.CosmosDbSample
{
    internal class GremlinQuery
    {
        public GremlinQuery(string description, string statement)
        {
            Description = description;
            Statement = statement;
        }

        public string Description { get; private set; }

        public string Statement { get; private set; }
    }

    internal class Program
    {
        private static string hostname = "az304204cosmsograph.gremlin.cosmos.azure.com";
        private static int port = 443;
        private static string authKey = "3g";

        //  private static string database = "graphdb";
        private static string collection = string.Empty;

        private static string DatabaseName = string.Empty;

        // private static string ContainerName = string.Empty;
        private static string PrimaryKey = "/partitionKey";

        private static async Task Main(string[] args)
        {
            string[] names = {
                "Hazel", "Madeline", "Isaac", "Shelia", "Christy", "Thelma", "Kara", "Johnnie", "Ron", "Frances",
                "Eddie", "Mona", "Jose", "Santos"
            };

            Console.WriteLine($"Enter the name of Graph Database :");
            DatabaseName = Console.ReadLine();

            Console.WriteLine($"Enter the name of Graph.Collection  :");
            collection = Console.ReadLine();

            // string ContainerName = "Online";
            string PrimaryKey = "/partitionKey";

            try
            {
                // Initialize Cosmos DB client
                CosmosClient cosmosClient = new CosmosClient("https://az304204cosmsograph.documents.azure.com:443/", authKey);

                // Create the database if it doesn't exist
                Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(DatabaseName);
                Console.WriteLine($"Created Database: {database.Id}");

                // Define throughput for the collection (graph)
                ContainerProperties containerProperties = new ContainerProperties(collection, "/partitionKey");

                // Create the graph (collection) if it doesn't exist
                Container container = await database.CreateContainerIfNotExistsAsync(containerProperties);
                Console.WriteLine($"Created Collection (Graph): {container.Id}");

                // Clean up
                cosmosClient.Dispose();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            var gremlinServer = new GremlinServer(
                hostname, port, enableSsl: true,
                username: "/dbs/" + DatabaseName + "/colls/" + collection,
                password: authKey);

            var queries = ConstructSociogram(names);
            await ExecuteGraphQueriesAsync(gremlinServer, queries);

            Console.WriteLine("Graph constructed. B-)");
            Console.ReadLine();
        }

        private static IEnumerable<GremlinQuery> ConstructSociogram(string[] names)
        {
            var queries = new List<GremlinQuery>();

            var rand = new Random();
            var vertexCount = names.Length;
            for (var i = 0; i < vertexCount; ++i)
            {
                // For each name, add a new vertex and add it to the beginning of the Gremlin command list.
                var name = names[i];
                queries.Insert(0, new GremlinQuery($"Add {name}", GetVertexStatement(name)));

                // Build up to 8 outgoing edges on the current name.
                // Get some random indices of other names...
                var targetIndices = Enumerable.Range(0, vertexCount - 1).OrderBy(x => rand.Next()).Take(8).Where(x => x != i);

                // ... and add an edge towards each of these people.
                foreach (var targetIndex in targetIndices)
                {
                    var targetName = names[targetIndex];
                    queries.Add(new GremlinQuery($"{name} knows {targetName}", GetEdgeStatement(name, targetName)));
                }
            }

            // Before executing any queries, we will reset the graph by dropping it.
            queries.Insert(0, new GremlinQuery("Drop existing Graph", "g.V().drop()"));
            return queries;
        }

        private static async Task ExecuteGraphQueriesAsync(GremlinServer gremlinServer, IEnumerable<GremlinQuery> queries)
        {
            Console.Write($"need to execute : {queries.Count()}.. ");
            using (var gremlinClient = new GremlinClient(gremlinServer, new GraphSON2Reader(), new GraphSON2Writer(), GremlinClient.GraphSON2MimeType))
            {
                int i = 1;
                foreach (var query in queries)
                {
                    try
                    {
                        Console.Write($"Executing {i}: {query.Description}.: {query.Statement}.. ");
                        var resultSet=  await gremlinClient.SubmitAsync<dynamic>(query.Statement);
                        if (resultSet.Count > 0)
                        {
                            Console.WriteLine("\tResult:");
                            foreach (var result in resultSet)
                            {
                                // The vertex results are formed as Dictionaries with a nested dictionary for their properties
                                string output = JsonConvert.SerializeObject(result);
                                Console.WriteLine($"\t{output}");
                            }
                            Console.WriteLine();
                            Console.WriteLine("ok");
                        }else
                        {
                            Console.WriteLine("error");
                        }
                        
                        i = i + 1;
                    }
                    catch (ResponseException e)
                    {
                        Console.WriteLine("\tRequest Error!");

                        // Print the Gremlin status code.
                        Console.WriteLine($"\tStatusCode: {e.StatusCode}");

                        // On error, ResponseException.StatusAttributes will include the common StatusAttributes for successful requests, as well as
                        // additional attributes for retry handling and diagnostics.
                        // These include:
                        //  x-ms-retry-after-ms         : The number of milliseconds to wait to retry the operation after an initial operation was throttled. This will be populated when
                        //                              : attribute 'x-ms-status-code' returns 429.
                        //  x-ms-activity-id            : Represents a unique identifier for the operation. Commonly used for troubleshooting purposes.
                        PrintStatusAttributes(e.StatusAttributes);
                        Console.WriteLine($"\t[\"x-ms-retry-after-ms\"] : {GetValueAsString(e.StatusAttributes, "x-ms-retry-after-ms")}");
                        Console.WriteLine($"\t[\"x-ms-activity-id\"] : {GetValueAsString(e.StatusAttributes, "x-ms-activity-id")}");

                        throw;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error: {ex.Message}");
                        break;
                    }
                }
            }
        }

        private static void PrintStatusAttributes(IReadOnlyDictionary<string, object> attributes)
        {
            Console.WriteLine($"\tStatusAttributes:");
            Console.WriteLine($"\t[\"x-ms-status-code\"] : {GetValueAsString(attributes, "x-ms-status-code")}");
            Console.WriteLine($"\t[\"x-ms-total-request-charge\"] : {GetValueAsString(attributes, "x-ms-total-request-charge")}");
        }

        public static string GetValueAsString(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            return JsonConvert.SerializeObject(GetValueOrDefault(dictionary, key));
        }

        public static object GetValueOrDefault(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            if (dictionary.ContainsKey(key))
            {
                return dictionary[key];
            }

            return null;
        }

        private static string GetVertexStatement(string name) => $"g.addV('person').property('id', '{name}').property('partitionKey', 'as')";

        private static string GetEdgeStatement(string from, string to) => $"g.V('{from}').addE('knows').to(g.V('{to}'))";
    }
}