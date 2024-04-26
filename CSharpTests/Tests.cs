using Akka;
using Akka.Actor;
using Akka.Cluster;
using Akka.Configuration;
using Akka.Event;
using Akka.Routing;
using Akka.TestKit.Xunit2;
using Xunit;
using Xunit.Abstractions;

namespace CSharpTests;

public static class PoolConfig
{
    public static Config Get(string systemName, int seedPort, int port) => 
        $$"""
             akka
             {
                 actor
                 {
                     provider = "Akka.Cluster.ClusterActorRefProvider, Akka.Cluster"
                     deployment
                     {
                         /echo
                         {
                             router = broadcast-pool
                             cluster
                             {
                                 enabled = on
                                 max-nr-of-instances-per-node = 1
                                 max-total-nr-of-instances = 2
                                 allow-local-routees = on
                                 use-role = Upload
                             }
                         }
                     }
                 }
                 
                 remote
                 {
                     dot-netty.tcp
                     {
                         public-hostname = localhost
                         hostname = localhost
                         port = {{port}}
                     }
                 }
                 
                 cluster
                 {
                     roles = ["Upload"]
                     seed-nodes = [ "akka.tcp://{{systemName}}@localhost:{{seedPort}}" ]
                     min-nr-of-members = 2
                 }
             }
             """;
}
    
sealed class EchoActor: UntypedActor
{
    private readonly UniqueAddress _selfAddress;
    private readonly ILoggingAdapter _log;
    private readonly IActorRef _testActor;
        
    public EchoActor(IActorRef testActor)
    {
        _selfAddress = Cluster.Get(Context.System).SelfUniqueAddress;
        _log = Context.GetLogger();
        _testActor = testActor;
    }

    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case "ping":
                _log.Info("Received ping on {0} from {1}", _selfAddress, Sender);
                _testActor.Tell("pong");
                break;
            default:
                Unhandled(message);
                break;
        }
    }
}

public class ClusterBroadcastPoolTests: TestKit
{
    private const string SystemName = "cluster-system-2";
    private const int SeedPort = 5000;
    private readonly ITestOutputHelper _out;
    private readonly Cluster _cluster;
    private readonly ActorSystem _system1;
    private readonly ActorSystem _system2;
    
    public ClusterBroadcastPoolTests(ITestOutputHelper output) : base(PoolConfig.Get(SystemName, SeedPort, SeedPort), SystemName, output)
    {
        _out = output;
        _system1 = ActorSystem.Create(SystemName, PoolConfig.Get(SystemName, SeedPort, SeedPort + 1));
        _system2 = ActorSystem.Create(SystemName, PoolConfig.Get(SystemName, SeedPort, SeedPort + 2));
        _cluster = Cluster.Get(_system1);
        
        InitializeLogger(_system1, "[SYS-1]");
        InitializeLogger(_system2, "[SYS-2]");
    }

    [Fact]
    public async Task ShouldBroadcastMessageToAllRoutees()
    {
        try
        {
            var tcs = new TaskCompletionSource<Done>();
            _cluster.RegisterOnMemberUp(() => { tcs.SetResult(Done.Instance); });
            await tcs.Task;

            var probe = CreateTestProbe();

            var propsWithRouter = Props.Create(() => new EchoActor(probe)).WithRouter(FromConfig.Instance);
            var pool = Sys.ActorOf(propsWithRouter, "echo");

            // wait until cluster pool stabilizes
            await Task.Delay(1000);
            
            pool.Tell("ping");
            await probe.ExpectMsgAsync("pong");
            await probe.ExpectMsgAsync("pong");
            await probe.ExpectNoMsgAsync(TimeSpan.FromSeconds(1));
        }
        finally
        {
            await Task.WhenAll(
                _system1.Terminate(),
                _system2.Terminate());
        }
    }
}
