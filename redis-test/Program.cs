using System.Net;
using NRedisStack;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
//...

// Get the start time and end time of the batch (you can replace these with your own values)
DateTime startTime = DateTime.Now; // Replace this with the actual start time

ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();

for (var i = 0; i <= 180000; i++)
{
    var user = new User()
    {
        Name = Faker.Name.FullName(),
        FirstName = Faker.Name.First(),
        LastName = Faker.Name.Last(),
        Address = Faker.Address.StreetAddress(),
        Email = Faker.Internet.Email()
    };

    var e = new HashEntry[]{
        new("name", user.Name),
        new("firstname", user.FirstName),
        new("lastname", user.LastName),
        new("address", user.Address),
        new("email", user.Email),
    };

    db.HashSet($"user:{user.Email}", e);
    Console.WriteLine($"user n°{i} stored");
}

DateTime endTimeIngest = DateTime.Now;   // Replace this with the actual end time
EndPoint endPoint = redis.GetEndPoints().First();
RedisKey[] keys = redis.GetServer(endPoint).Keys(database: db.Database, pattern: "user:*").ToArray();
var batches = new List<RedisKey[]>();

var tasks = new List<Task>();
var batchSize = keys.Length / Environment.ProcessorCount;

DateTime startTimeM = DateTime.Now;
for (int i = 0; i < Environment.ProcessorCount; i++)
{
    var batch = keys.Skip(i * batchSize).Take(batchSize).ToArray();
    batches.Add(batch);

    tasks.Add(Task.Run(() =>
    {
        foreach (var item in batch)
        {
            var email = db.HashGet(item, "email");
            Console.WriteLine($"mail: {email}");
        }
    }));
}

await Task.WhenAll(tasks);
DateTime endTimeM = DateTime.Now;

// Calculate the time difference
TimeSpan elapsedTime = endTimeIngest - startTime;
TimeSpan elapsedTime2 = endTimeM - startTimeM;


// Display the elapsed time
Console.WriteLine("Ingest started at: " + startTime);
Console.WriteLine("Ingest ended at: " + endTimeIngest);
Console.WriteLine("Ingest time: " + elapsedTime);
Console.WriteLine("MultiThreads started at: " + startTime);
Console.WriteLine("MultiThreads ended at: " + endTimeM);
Console.WriteLine("MultiThreads time: " + elapsedTime2);