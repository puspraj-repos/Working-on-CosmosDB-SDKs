using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace CosmosDB
{
    class Program
    {
        private static string _connection_string = "Conection_string_for_cosmos_db";
        private static string _database_name = "knight-test";
        private static string _container_name="customer";
        private static string _partition_key = "/customerid";
        static void Main(string[] args)
        {
            createDatabase();
            addItem();
            addBulkItem();
            readItem();
            getAndUpdateItem();
            deleteItem();
            executeStoredProc();
            addItemStoredProc();
            createdTrigger();
            addFromJsonAsync();
            Console.ReadKey();
        }
        public static void createDatabase()
        {
            CosmosClient _client = new CosmosClient(_connection_string);
            _client.CreateDatabaseAsync(_database_name).GetAwaiter().GetResult();
            //getting handle of database
            Database _database = _client.GetDatabase(_database_name);
            _database.CreateContainerAsync(_container_name, _partition_key).GetAwaiter().GetResult();
            Console.WriteLine("Database Created");
        }

        public static void addItem()
        {

            CosmosClient _client = new CosmosClient(_connection_string);
            Container _container = _client.GetContainer(_database_name, _container_name);
            Course course = new Course()
            {
                id = "1",
                customerid = "C1",
                coursename = "C++",
                rating = 4.6m,
                orders = new List<Order>() { new Order { orderid="O1", ordername="N1"}, new Order { ordername="N2", orderid="O2"} }
            };
            _container.CreateItemAsync<Course>(course, new PartitionKey(course.customerid)).GetAwaiter().GetResult();
            Console.WriteLine("Item Created");
        }
        public static void addBulkItem()
        {
            CosmosClient _client = new CosmosClient(_connection_string, new CosmosClientOptions() { AllowBulkExecution = true });

            List<Course> courses = new List<Course>()
            {
                new Course(){coursename = "Java", customerid="C02", id="2"},
                new Course(){coursename="Python", customerid="C02", id="3"},
                new Course(){coursename=".NET", customerid="C02", id="2"}
            };
            List<Task> _task = new List<Task>();
            foreach(Course course in courses)
            {
                _client.GetContainer(_database_name, _container_name).CreateItemAsync(course, new PartitionKey(course.customerid));
            }
            Task.WhenAll().GetAwaiter().GetResult();
            Console.WriteLine("Items Created");
        }
        public static void readItem()
        {
            CosmosClient _client = new CosmosClient(_connection_string);
            Container _container = _client.GetContainer(_database_name, _container_name);
            string query = "SELECT * FROM c WHERE c.customerid= 'C02'";
            QueryDefinition _definition = new QueryDefinition(query);
            FeedIterator<Course> _iterator = _container.GetItemQueryIterator<Course>(_definition);

            while(_iterator.HasMoreResults)
            {
                FeedResponse<Course> _response = _iterator.ReadNextAsync().GetAwaiter().GetResult();
                foreach(Course course in _response)
                {
                    Console.WriteLine(course.coursename);
                    Console.WriteLine(course.id);
                    Console.WriteLine(course.customerid);
                }
            }
        }
        public static void getAndUpdateItem()
        {
            CosmosClient _client = new CosmosClient(_connection_string);
            Container _container = _client.GetContainer(_database_name, _container_name);
            string id = "1", partitionKey = "C1";
            ItemResponse<Course> _response = _container.ReadItemAsync<Course>(id, new PartitionKey(partitionKey)).GetAwaiter().GetResult();
            Course course= _response.Resource;
            Console.WriteLine(course.coursename);
            Console.WriteLine(course.id);
            course.coursename = "Ruby";
            _container.ReplaceItemAsync<Course>(course, id, new PartitionKey(partitionKey)).GetAwaiter().GetResult();

        }
        public static void deleteItem()
        {
            CosmosClient _client = new CosmosClient(_connection_string);
            Container _container = _client.GetContainer(_database_name, _container_name);
            _container.DeleteItemAsync<Course>("1", new PartitionKey("C01")).GetAwaiter().GetResult();
        }
        public static void executeStoredProc()
        {
            CosmosClient _client = new CosmosClient(_connection_string);
            Container _container = _client.GetContainer(_database_name, _container_name);
            try
            {
                string output = _container.Scripts.ExecuteStoredProcedureAsync<string>("sample", new PartitionKey(String.Empty), null).GetAwaiter().GetResult();
                Console.WriteLine(output);
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        public static void addItemStoredProc()
        {
            CosmosClient _client = new CosmosClient(_connection_string);
            Container _container = _client.GetContainer(_database_name, _container_name);
            dynamic[] arr = new dynamic[]
            {
                new {id = "9", customerid="C05", coursename="C sharp", rating=4.7m},
                new {id="8", customerid="C05", coursename="C++", rating=4.8m}
            };
            try
            {
                string _output = _container.Scripts.ExecuteStoredProcedureAsync<string>("addItems", new PartitionKey("C05"), new[] { arr }).GetAwaiter().GetResult();
                Console.WriteLine(_output);
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        public static void createdTrigger()
        {
            CosmosClient _client = new CosmosClient(_connection_string);
            Container _container = _client.GetContainer(_database_name, _container_name);
            Course obj = new Course()
            {
                id = "7",
                coursename = "PHP",
                customerid = "C05",
                rating = 4.5m
            };
            try
            {
                _container.CreateItemAsync<Course>(obj, new PartitionKey(obj.customerid), new ItemRequestOptions() { PreTriggers = new List<string>() { "addTimestamp" } }).GetAwaiter().GetResult();
                Console.WriteLine("Item created");
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        public static async Task addFromJsonAsync()
        {
            FileStream _fs = new FileStream(System.Environment.CurrentDirectory + @"/json1.json", FileMode.Open, FileAccess.Read);
            StreamReader _reader = new StreamReader(_fs);
            JsonTextReader _json_reader = new JsonTextReader(_reader);
            CosmosClient _client = new CosmosClient(_connection_string);
            //_client.CreateDatabaseAsync("fromjson").GetAwaiter().GetResult();
            //Database _database = _client.GetDatabase("fromjson");
            //_database.CreateContainerAsync("persons", "/gender").GetAwaiter().GetResult();
            Container _container = _client.GetContainer("fromjson", "persons");
            
            while(_json_reader.Read())
            {
                if(_json_reader.TokenType==JsonToken.StartObject)
                {
                    JObject _object = JObject.Load(_json_reader);
                    Person person = _object.ToObject<Person>();
                    person.id = Guid.NewGuid().ToString();
                    await _container.CreateItemAsync<Person>(person, new PartitionKey(person.gender));
                    Console.WriteLine($"Created item {person.id}");
                }
            }
        }

        public static void addFromJson()
        {
            FileStream _fs = new FileStream(System.Environment.CurrentDirectory + @"/json1.json", FileMode.Open, FileAccess.Read);
            StreamReader _reader = new StreamReader(_fs);
            JsonTextReader _json_reader = new JsonTextReader(_reader);
            CosmosClient _client = new CosmosClient(_connection_string);
            //_client.CreateDatabaseAsync("fromjson").GetAwaiter().GetResult();
            //Database _database = _client.GetDatabase("fromjson");
            //_database.CreateContainerAsync("persons", "/gender").GetAwaiter().GetResult();
            Container _container = _client.GetContainer(_database_name, _container_name);

            try
            {

                while (_json_reader.Read())
                {
                    if (_json_reader.TokenType == JsonToken.StartObject)
                    {
                        JObject _object = JObject.Load(_json_reader);
                        Person person = _object.ToObject<Person>();
                        person.id = Guid.NewGuid().ToString();
                        _container.CreateItemAsync<Person>(person, new PartitionKey(person.gender)).GetAwaiter().GetResult();
                        Console.WriteLine($"Created item {person.id}");


                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }

}
