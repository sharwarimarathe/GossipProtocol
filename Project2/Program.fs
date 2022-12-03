#if INTERACTIVE
#time "on"
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open Akka.TestKit
#endif 

open System
open System.Collections.Generic
open System
open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Diagnostics

type MsgType=
    | Alert 
    | Process1
    | Process2
    | NeighbourRef of IActorRef []
    | InitialSum of int
    | Time of int
    | TotalNodes of int
    | Adder
    | MainPushSum of Double * Double * String


[<EntryPoint>]
let main argv =
    let mutable nodes = ((argv.[0])|>string)|>int
    let topology = argv.[1]
    let algorithm = argv.[2]
    let timer = Diagnostics.Stopwatch()
    let system = ActorSystem.Create("System")
    let dictionary = new Dictionary<IActorRef, bool>()

    let mutable actorNodes=[||] 

    let scheduler(mailbox: Actor<_>) = //counts the number of terminated actor nodes and calculates convergence time 
    
        let mutable counter = 0
        let mutable start = 0
        

        let rec loop()= actor{
            let! msg = mailbox.Receive();
            match msg with
            | Time startVar -> start <- startVar
            | Alert -> 
                counter <- counter + 1
                if counter = nodes then // checks if the number of terminated actor nodes equal total nodes
                    timer.Stop()
                    printfn "Convergence Time: %f ms" timer.Elapsed.TotalMilliseconds
                    Environment.Exit(0)
            | _ -> ()

            return! loop()
        }            
        loop()
    
    let scheduler = spawn system "scheduler" scheduler

    let Actor1(mailbox: Actor<_>) =
        let mutable checkConverge= false
        let mutable count = 1
        let mutable rumours = 0 // keeps tab of the number of times rumour was encountered
        let mutable sum = 0 |>double//sum of the current actor
        let mutable neighbours: IActorRef [] = [||] // keeps the actor reference of the neighbouring nodes
        let mutable weight = 1.0//weight of the current actor

        let rec loop()= actor{
            let! msg = mailbox.Receive();
   
            match msg with
            | InitialSum sumRef ->
                    sum <- sumRef |> double

            | NeighbourRef ref ->
                neighbours <- ref


            | Process1 ->
                if rumours < 11 then
                    let ind = Random().Next(0, neighbours.Length)
                    if not dictionary.[neighbours.[ind]] then
                        neighbours.[ind] <! Process2
                    mailbox.Self <! Process1

            | Process2 ->
                if (rumours = 10) then
                    dictionary.[mailbox.Self] <- true
                    scheduler <! Alert
                if rumours = 0 then 
                    mailbox.Self <! Process1
                
                rumours <- rumours + 1


            | MainPushSum(s: float, w, checker) ->
                if checker="start" then //starts the pushsum algorithm
                    let startNode = Random().Next(0, neighbours.Length)
                    sum <- sum / 2.0
                    weight <- weight / 2.0
                    neighbours.[startNode] <! MainPushSum(sum, weight,"notstart")
                else // goes through the neighbours
                    let sumN = sum + s
                    sum<-sumN
                    let weightN = weight + w
                    sum<-sumN
                    weight<-weightN

                    let difference = sumN / weightN - sum / weight  |> abs
                    if not checkConverge then
                                if difference > 10.0 ** -10.0 then 
                                    count <- 0
                                else 
                                    count <- count + 1

                                if  count = 3 then //checks if actors ratios did not change more than 10**-10 in 3 rounds
                                    checkConverge <- true
                                    scheduler <! Alert
                    
                                sum <- sum / 2.0
                                weight <- weight / 2.0
                                
                                neighbours.[Random().Next(0, neighbours.Length)] <! MainPushSum(sum, weight,checker)
                    else
                        let newNode = Random().Next(0, neighbours.Length)
                        neighbours.[newNode] <! MainPushSum(s, w,checker)


            | _ -> ()
            return! loop()
        }            
        loop()



    let Actor2 (mailbox: Actor<_>) =
        let neighbors = new List<IActorRef>()
        let rec loop() = actor {
            let! msg = mailbox.Receive()
            match msg with 
            | Adder _ ->
                for i in [0..nodes-1] do
                        neighbors.Add actorNodes.[i]
                mailbox.Self <! Process1
            | Process1 ->
                if neighbors.Count > 0 then
                    let indexRand = Random().Next(neighbors.Count)

                    if not (dictionary.[neighbors.[indexRand]]) then
                        neighbors.[indexRand] <! Process2
                        
                    else
                        (neighbors.Remove neighbors.[indexRand]) |>ignore
                    mailbox.Self <! Process1
            | _ -> ()
            return! loop()
        }
        loop()



    let actorGossip = spawn system "gossip" Actor2

    let initializeNeighbours =
        actorNodes <- Array.zeroCreate (nodes+1)
        for x in [0..nodes] do
            let actorRef = spawn system (string(x)) Actor1
            actorNodes.[x] <- actorRef 
            actorNodes.[x] <! InitialSum x
            dictionary.Add(actorNodes.[x], false)
        actorNodes

    match topology with

        |"line" ->
            let a=initializeNeighbours 
            for i in [ 0 .. nodes ] do 
                let mutable arr1 = [||]
                if i = 0 then
                    arr1 <- (Array.append arr1[|actorNodes.[i+1]|])
                elif i = nodes then
                    arr1 <- (Array.append arr1 [|actorNodes.[i-1]|])
                else 
                    arr1 <- (Array.append arr1 [| actorNodes.[(i - 1)] ; actorNodes.[(i + 1 ) ] |] ) 
        
                actorNodes.[i] <! NeighbourRef(arr1)

            let choice = Random().Next(0, nodes)
            timer.Start()
            match algorithm with
            | "gossip" -> 
                scheduler <! Time(DateTime.Now.TimeOfDay.Milliseconds)
                printfn "Gossip Starts"
                actorNodes.[choice] <! Process1
                actorGossip<! Adder
        
            | "push-sum" -> 
                scheduler <! Time(DateTime.Now.TimeOfDay.Milliseconds)
                printfn "Push Sum Starts"
                actorNodes.[choice] <! MainPushSum(0.0,1.0,"start")    
            | _ ->
                printfn "option invalid"   

        | "full" ->
            let a=initializeNeighbours
            for i in [ 0 .. nodes ] do
                let mutable arr1 = [||]
                for j in [0..nodes] do 
                    if i <> j then // checks that the current node isn't assigned itself as the neighbour
                        arr1 <- (Array.append arr1 [|actorNodes.[j]|])
                actorNodes.[i]<! NeighbourRef(arr1)
              

            timer.Start()
            //Choose a random worker to start the gossip
            let choice = Random().Next(0, nodes)

            match algorithm with
            | "gossip" -> 
                
                scheduler <! Time(DateTime.Now.TimeOfDay.Milliseconds)
                printfn "Gossip Starts"
                actorNodes.[choice] <! Process1
            | "push-sum" -> 
                
                scheduler <! Time(DateTime.Now.TimeOfDay.Milliseconds)
                printfn "Push Sum Starts"
                actorNodes.[choice] <! MainPushSum(0.0,1.0,"start")   
            | _ ->
                printfn "option invalid"


        |"3D" | "Imp3D" ->
            nodes<-((nodes|>float|> System.Math.Cbrt)|>ceil ) ** 3.0 |> int
            let grid = nodes |> float |> System.Math.Cbrt |> ceil |> int  
            let a=initializeNeighbours 
            for i in [ 0 .. (grid-1)] do //checks the plane
                for j in [ 0 .. (grid-1) ] do//checks the row
                    for k in [ 0 .. (grid-1) ] do//checks the column
                        let mutable neighbours: IActorRef [] = [||]
                        if i - 1 >= 0 then
                            neighbours <- Array.append neighbours [| actorNodes.[(i - 1 ) * (grid*grid) + j * grid + k ] |]
                        if  i + 1 < grid then
                            neighbours <- Array.append neighbours [| actorNodes.[(i + 1 ) * (grid*grid) + j * grid+ k] |]
                        if j + 1 < grid then
                            neighbours <- Array.append neighbours [| actorNodes.[i * (grid * grid) + (j + 1)*grid+k] |]
                        if  j - 1 >= 0 then 
                            neighbours <- Array.append neighbours [| actorNodes.[i * (grid * grid) + (j - 1)*grid+k] |]
                        if k+1 < grid then
                            neighbours <- Array.append neighbours [| actorNodes.[i * (grid * grid) + j * grid+k+1] |]
                        if k-1 >= 0 then
                            neighbours <- Array.append neighbours [| actorNodes.[i * (grid * grid) + j * grid + k- 1] |]
                        if topology<>"3D" then
                            let ind = Random().Next(0, nodes-1)
                            neighbours <- Array.append neighbours [|actorNodes.[ind]|]
                        actorNodes.[i * (grid*grid) + j*grid + k] <! NeighbourRef(neighbours)
        
            let choice = Random().Next(0, nodes)
            timer.Start()
            match algorithm with 
            | "gossip" -> 
                
                scheduler <! Time(DateTime.Now.TimeOfDay.Milliseconds)
                printfn "Gossip Starts"
                actorNodes.[choice] <! Process1
                actorGossip<! Adder
            | "push-sum" -> 
                
                scheduler <! Time(DateTime.Now.TimeOfDay.Milliseconds)
                printfn "Push Sum Starts"
                actorNodes.[choice] <! MainPushSum(0.0,1.0,"start")  
            | _ -> 
                printfn "option invalid"
        | _ -> ()

    
    System.Console.ReadKey() |> ignore
    0 // return an integer exit code