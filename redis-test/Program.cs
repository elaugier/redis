using System.Net;
using NRedisStack;
using StackExchange.Redis;

ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();

DateTime startTime = DateTime.Now;
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
DateTime endTimeIngest = DateTime.Now;

EndPoint endPoint = redis.GetEndPoints().First();
RedisKey[] keys = redis.GetServer(endPoint).Keys(database: db.Database, pattern: "user:*").ToArray();
var tasks = new List<Task>();
var batchSize = keys.Length / Environment.ProcessorCount;

DateTime startTimeM = DateTime.Now;
for (int i = 0; i < Environment.ProcessorCount; i++)
{
    var batch = keys.Skip(i * batchSize).Take(batchSize).ToArray();
    var taskCounter = i;

    tasks.Add(Task.Run(async () =>
    {
        var count = 0;
        foreach (var item in batch)
        {
            var email = await db.HashGetAsync(item, "email");
            await db.HashSetAsync(item, new HashEntry[]{
                new("email", Faker.Internet.Email()),
                new("address", Faker.Address.StreetAddress()),
            });
            count++;
            Console.WriteLine($"Task {taskCounter} => mail: {email}");
        }
        Console.WriteLine($"End Task n°{taskCounter} count: {count}");
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
