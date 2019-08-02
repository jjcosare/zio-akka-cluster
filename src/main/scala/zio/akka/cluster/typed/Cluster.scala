package zio.akka.cluster.typed

import java.util.concurrent.TimeUnit

import akka.actor.Address
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.ClusterEvent.{ClusterDomainEvent, CurrentClusterState, MemberEvent}
import akka.cluster.typed.{JoinSeedNodes, Leave, Subscribe}
import akka.util.Timeout
import zio.Exit.{Failure, Success}
import zio.{Queue, Runtime, Task, ZIO}

object Cluster {

  private val cluster: ZIO[ActorSystem[_], Throwable, akka.cluster.typed.Cluster] =
    for {
      actorSystem <- ZIO.environment[ActorSystem[_]]
      cluster     <- Task(akka.cluster.typed.Cluster(actorSystem))
    } yield cluster

  /**
   *  Returns the current state of the cluster.
   */
  val clusterState: ZIO[ActorSystem[_], Throwable, CurrentClusterState] =
    for {
      cluster <- cluster
      state   <- Task(cluster.state)
    } yield state

  /**
   *  Joins a cluster using the provided seed nodes.
   */
  def join(seedNodes: List[Address]): ZIO[ActorSystem[_], Throwable, Unit] =
    for {
      cluster <- cluster
      _       <- Task(cluster.manager ! JoinSeedNodes(seedNodes))
    } yield ()

  /**
   *  Leaves the current cluster.
   */
  val leave: ZIO[ActorSystem[_], Throwable, Unit] =
    for {
      cluster <- cluster
      _       <- Task(cluster.manager ! Leave(cluster.selfMember.address))
    } yield ()

  /**
   *  Subscribes to the current cluster events. It returns an unbounded queue that will be fed with cluster events.
   *  `initialStateAsEvents` indicates if you want to receive previous cluster events leading to the current state, or only future events.
   *  To unsubscribe, use `queue.shutdown`.
   *  To use a bounded queue, see `clusterEventsWith`.
   */
  def clusterEvents(initialStateAsEvents: Boolean = false): ZIO[ActorSystem[_], Throwable, Queue[ClusterDomainEvent]] =
    Queue.unbounded[ClusterDomainEvent].tap(clusterEventsWith(_, initialStateAsEvents))

  /**
   *  Subscribes to the current cluster events, using the provided queue to push the events.
   *  `initialStateAsEvents` indicates if you want to receive previous cluster events leading to the current state, or only future events.
   *  To unsubscribe, use `queue.shutdown`.
   */
  def clusterEventsWith(queue: Queue[ClusterDomainEvent],
                        initialStateAsEvents: Boolean = false): ZIO[ActorSystem[_], Throwable, Unit] =
    for {
      rts         <- Task.runtime[Any]
      actorSystem <- ZIO.environment[ActorSystem[_]]
      _           <- Task({
        implicit val timeout = Timeout(30, TimeUnit.SECONDS)
        actorSystem.systemActorOf(new SubscriberActor(rts, queue, initialStateAsEvents).behavior(), "SubscriberActor")
      })
    } yield ()

  private[cluster] class SubscriberActor(rts: Runtime[Any],
                                         queue: Queue[ClusterDomainEvent],
                                         initialStateAsEvents: Boolean)
  {
    def behavior(): Behavior[ClusterDomainEvent] =
      Behaviors.setup { context =>

//        NOTE: no constructor for Subscription that receives an initialState on akka typed
//        val initialState: SubscriptionInitialStateMode =
//          if (initialStateAsEvents) InitialStateAsEvents else InitialStateAsSnapshot

        akka.cluster.typed.Cluster(context.system).subscriptions ! Subscribe(context.self, classOf[MemberEvent])
        Behaviors.receiveMessage {
          case ev: ClusterDomainEvent =>
            rts.unsafeRunAsync(queue.offer(ev)) {
              case Success(_)     => Behaviors.same
              case Failure(cause) => if (cause.interrupted) Behaviors.stopped // stop listening if the queue was shut down
            }
            Behaviors.same
          case _ => Behaviors.same
        }
      }
  }

}
