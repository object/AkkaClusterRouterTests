namespace FSharpTests

module ClusterPoolTests =

    open System.Threading.Tasks
    open Akka.Routing
    open Akka.Cluster
    open Akka.FSharp
    open Xunit
    open Xunit.Abstractions

    type ClusterPoolTests(output: ITestOutputHelper) =

        let configWithPortJsonSerializer port =
            Configuration.parse (
                """
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
                                    use-role = worker
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
                            port = """
                + port.ToString()
                + """
                        }
                    }
                    
                    cluster
                    {
                        roles = ["worker"]
                        seed-nodes = [ "akka.tcp://cluster-system-1@localhost:5010" ]
                        min-nr-of-members = 2
                    }
                }
            """
            )

        let configWithPortHyperionSerializer port =
            Configuration.parse (
                """
                akka
                {
                    actor
                    {
                        provider = "Akka.Cluster.ClusterActorRefProvider, Akka.Cluster"
                        serializers {
                         hyperion = "Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion"
                        }
                        serialization-bindings {
                          "System.Object" = hyperion
                        }
                        serialization-identifiers {
                          "Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion" = -5
                        }
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
                                    use-role = worker
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
                            port = """
                + port.ToString()
                + """
                        }
                    }
                    
                    cluster
                    {
                        roles = ["worker"]
                        seed-nodes = [ "akka.tcp://cluster-system-2@localhost:5020" ]
                        min-nr-of-members = 2
                    }
                }
            """
            )

        [<Fact>]
        member _.``Should broadcast message to all routees (JSON serializer)``() =

            let system1 = System.create "cluster-system-1" (configWithPortJsonSerializer 5010)
            let system2 = System.create "cluster-system-1" (configWithPortJsonSerializer 5011)

            let testKit = new Akka.TestKit.Xunit2.TestKit(system1, output)
            new Akka.TestKit.Xunit2.TestKit(system2, output) |> ignore

            let echoActor testActor (mailbox: Actor<string>) =
                let nodeAddress = Cluster.Get(mailbox.Context.System).SelfUniqueAddress
                let rec loop () =
                    actor {
                        let! msg = mailbox.Receive()
                        match msg with
                        | "ping" ->
                            logDebug mailbox $"Received ping on {nodeAddress} from {mailbox.Sender().Path}"
                            testActor <! "pong"
                        | _ -> ()
                        return! loop ()
                    }
                loop ()

            let cluster = Cluster.Get system1
            let tcs = TaskCompletionSource<Akka.Done>()
            cluster.RegisterOnMemberUp(fun () -> tcs.SetResult(Akka.Done.Instance))
            tcs.Task |> Async.AwaitTask |> Async.RunSynchronously |> ignore

            let pool = spawnOpt system1 "echo" (echoActor testKit.TestActor) [ SpawnOption.Router(FromConfig.Instance) ]

            Async.Sleep 1000 |> Async.RunSynchronously

            pool <! "ping"
            testKit.ReceiveN 2 |> ignore
            testKit.ExpectNoMsg

        [<Fact>]
        member _.``Should broadcast message to all routees (Hyperion serializer)``() =

            let system1 = System.create "cluster-system-2" (configWithPortHyperionSerializer 5020)
            let system2 = System.create "cluster-system-2" (configWithPortHyperionSerializer 5021)

            let testKit = new Akka.TestKit.Xunit2.TestKit(system1, output)
            new Akka.TestKit.Xunit2.TestKit(system2, output) |> ignore

            let echoActor testActor (mailbox: Actor<string>) =
                let nodeAddress = Cluster.Get(mailbox.Context.System).SelfUniqueAddress
                let rec loop () =
                    actor {
                        let! msg = mailbox.Receive()
                        match msg with
                        | "ping" ->
                            logDebug mailbox $"Received ping on {nodeAddress} from {mailbox.Sender().Path}"
                            testActor <! "pong"
                        | _ -> ()
                        return! loop ()
                    }
                loop ()

            let cluster = Cluster.Get system1
            let tcs = TaskCompletionSource<Akka.Done>()
            cluster.RegisterOnMemberUp(fun () -> tcs.SetResult(Akka.Done.Instance))
            tcs.Task |> Async.AwaitTask |> Async.RunSynchronously |> ignore

            let pool = spawnOpt system1 "echo" (echoActor testKit.TestActor) [ SpawnOption.Router(FromConfig.Instance) ]

            Async.Sleep 1000 |> Async.RunSynchronously

            pool <! "ping"
            testKit.ReceiveN 2 |> ignore
            testKit.ExpectNoMsg

    type ClusterGroupTests(output: ITestOutputHelper) =

        let configWithPort port =
            Configuration.parse (
                """
                akka
                {
                    actor
                    {
                        provider = "Akka.Cluster.ClusterActorRefProvider, Akka.Cluster"
                        deployment
                        {
                            /echo {
                                router = broadcast-pool
                                max-nr-of-instances = 1
                            }
                            /echo_group
                            {
                                router = broadcast-group
                                routees.paths = ["/user/echo"]
                                cluster
                                {
                                    enabled = on
                                    max-nr-of-instances-per-node = 1
                                    max-total-nr-of-instances = 2
                                    allow-local-routees = on
                                    use-role = worker
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
                            port = """
                          + port.ToString()
                          + """
                        }
                    }
                    
                    cluster
                    {
                        roles = ["worker"]
                        seed-nodes = [ "akka.tcp://cluster-system-3@localhost:5030" ]
                        min-nr-of-members = 2
                    }
                }
            """
            )

        [<Fact>]
        member _.``Should broadcast message to all routees``() =

            let system1 = System.create "cluster-system-3" (configWithPort 5030)
            let system2 = System.create "cluster-system-3" (configWithPort 5031)

            let testKit = new Akka.TestKit.Xunit2.TestKit(system1, output)
            new Akka.TestKit.Xunit2.TestKit(system2, output) |> ignore

            let echoActor (mailbox: Actor<string>) =
                let nodeAddress = Cluster.Get(mailbox.Context.System).SelfUniqueAddress
                let rec loop () =
                    actor {
                        let! msg = mailbox.Receive()
                        match msg with
                        | "ping" ->
                            logDebug mailbox $"Received ping on {nodeAddress} from {mailbox.Sender().Path}"
                            testKit.TestActor <! "pong"
                        | _ -> ()
                        return! loop ()
                    }
                loop ()

            let cluster = Cluster.Get system1
            let tcs = TaskCompletionSource<Akka.Done>()
            cluster.RegisterOnMemberUp(fun () -> tcs.SetResult(Akka.Done.Instance))
            tcs.Task |> Async.AwaitTask |> Async.RunSynchronously |> ignore

            spawnOpt system1 "echo" echoActor [ SpawnOption.Router(FromConfig.Instance) ] |> ignore
            spawnOpt system2 "echo" echoActor [ SpawnOption.Router(FromConfig.Instance) ] |> ignore
            let group = system1.ActorOf(Akka.Actor.Props.Empty.WithRouter(FromConfig.Instance), "echo_group")

            Async.Sleep 1000 |> Async.RunSynchronously

            group <! "ping"
            testKit.ReceiveN 2 |> ignore
            testKit.ExpectNoMsg
