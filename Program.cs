using System.Text.Json;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoTest
{

	class Person
	{
		public string Name { get; set; }
		public string LastName { get; set; }

		public override string ToString()
		{
			return $"Name: {Name} Surname: {LastName}";
		}
	}

	interface NoSQLCommands
	{
		Task Put<T>(string table, string key, string value, T input);
		Task<T> Get<T>(string table, string key, string value);
	}

	class DynamoDb : NoSQLCommands, IDisposable
	{
		private RegionEndpoint _region;
		private AmazonDynamoDBClient _client;

		public DynamoDb(RegionEndpoint region) {
			_region = region;
			_client = new AmazonDynamoDBClient(region);
		}

		public void Dispose()
		{
			_client.Dispose();
		}

		public async Task<T> Get<T>(string table, string key, string value)
		{
			var result = await _client.GetItemAsync(table, new Dictionary<string, AttributeValue>
			{
				{key, new AttributeValue {S = value} }
			});

			var json = result.Item["value"].S;
			var item = JsonSerializer.Deserialize<T>(json);
			if (item == null)
			{
				throw new InvalidOperationException("Serialisation result was null.");
			}
			return item;
		}

		public async Task Put<T>(string table, string key, string value, T input)
		{
			string json = JsonSerializer.Serialize(input);

			var item = new Dictionary<string, AttributeValue>
			{
				{ key, new AttributeValue() { S = value } },
				{ "value", new AttributeValue() { S = json}}
			};

			await _client.PutItemAsync(table, item);
		}
	}

	/// <summary>
	/// Not this code assumes alot and has little error handling its just an example
	/// to practice with dynamo db.
	/// </summary>
	public class DynamoTest
	{
		public static async Task Main(string[] args)
		{
			var person = new Person
			{
				Name = "Ashton",
				LastName = "Ang"
			};

			var p2 = new Person
			{
				Name = "John",
				LastName = "Doe"
			};

			var region = RegionEndpoint.AFSouth1;

			using (var dynamoDb = new DynamoDb(region))
			{
				await dynamoDb.Put("test", "key", "Ashton", person);
				await dynamoDb.Put("test", "key", "John", p2);

				var res = await dynamoDb.Get<Person>("test", "key", "John");
				Console.WriteLine(res);
			}
		}
	}
}
