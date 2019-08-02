package zio.akka.cluster.sharding.typed

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityTypeKey}
import zio.akka.cluster.sharding.{Entity, MessageEnvelope}
import zio.akka.cluster.sharding.MessageEnvelope.{MessagePayload, PoisonPillPayload}
import zio.{Ref, Runtime, Task, UIO, ZIO}

/**
  *  A `Sharding[M]` is able to send messages of type `M` to a sharded entity or to stop one.
  */
trait Sharding[M] {

  def send(entityId: String, data: M): Task[Unit]

  def stop(entityId: String): Task[Unit]

}

object Sharding {

  /**
    *  Starts cluster sharding on this node for a given entity type.
    *
    * @param name the name of the entity type
    * @param onMessage the behavior of the entity when it receives a message
    * @param numberOfShards a fixed number of shards
    * @return a [[Sharding]] object that can be used to send messages to sharded entities
    */
  def start[Msg, State](
                         name: String,
                         onMessage: Msg => ZIO[Entity[State], Nothing, Unit],
                         numberOfShards: Int = 100
                       ): ZIO[ActorSystem[Msg], Throwable, Sharding[Msg]] =
    for {
      rts         <- ZIO.runtime[ActorSystem[Msg]]
      actorSystem = rts.Environment
      shardingRegion <- Task(
        ClusterSharding(actorSystem).init(
          akka.cluster.sharding.typed.scaladsl.Entity(
            typeKey = EntityTypeKey[MessageEnvelope](name),
            createBehavior = _ => new ShardEntity(rts)(onMessage).behavior
          )
        )
      )
    } yield
      new ShardingImpl[Msg] {
        override val getShardingRegion: ActorRef[ShardingEnvelope[MessageEnvelope]] = shardingRegion
      }

//  NOTE: no support for cluster sharding proxy on akka typed
//  /**
//    *  Starts cluster sharding in proxy mode for a given entity type.
//    *
//    * @param name the name of the entity type
//    * @param role an optional role to specify that this entity type is located on cluster nodes with a specific role
//    * @param numberOfShards a fixed number of shards
//    * @return a [[Sharding]] object that can be used to send messages to sharded entities on other nodes
//    */
//  def startProxy[Msg](
//                       name: String,
//                       role: Option[String],
//                       numberOfShards: Int = 100
//                     ): ZIO[ActorSystem, Throwable, Sharding[Msg]] =
//    for {
//      rts         <- ZIO.runtime[ActorSystem]
//      actorSystem = rts.Environment
//      shardingRegion <- Task(
//        ClusterSharding(actorSystem).startProxy(
//          typeName = name,
//          role,
//          extractEntityId = {
//            case MessageEnvelope(entityId, payload) =>
//              payload match {
//                case MessageEnvelope.PoisonPillPayload    => (entityId, PoisonPill)
//                case p: MessageEnvelope.MessagePayload[_] => (entityId, p)
//              }
//          },
//          extractShardId = {
//            case msg: MessageEnvelope => (math.abs(msg.entityId.hashCode) % numberOfShards).toString
//          }
//        )
//      )
//    } yield
//      new ShardingImpl[Msg] {
//        override val getShardingRegion: ActorRef = shardingRegion
//      }

  private[sharding] trait ShardingImpl[Msg] extends Sharding[Msg] {

    val getShardingRegion: ActorRef[ShardingEnvelope[MessageEnvelope]]

    override def send(entityId: String, data: Msg): Task[Unit] =
      Task(getShardingRegion ! ShardingEnvelope(entityId, MessagePayload(data)))

    override def stop(entityId: String): Task[Unit] =
      Task(getShardingRegion ! ShardingEnvelope(entityId, PoisonPillPayload))
  }

  private[sharding] class ShardEntity[Msg, State](rts: Runtime[Any])(
    onMessage: Msg => ZIO[Entity[State], Nothing, Unit]
  ) {

    def behavior: Behavior[MessageEnvelope] =
      Behaviors.setup { context =>

        val ref: Ref[Option[State]]    = rts.unsafeRun(Ref.make[Option[State]](None))
        val entity: Entity[State] = new Entity[State] {
          override def id: String                = context.self.path.name
          override def state: Ref[Option[State]] = ref
          override def stop: UIO[Unit]           = UIO(context.stop(context.self))
        }

        Behaviors.receiveMessage {
          case MessagePayload(msg) =>
            rts.unsafeRunSync(onMessage(msg.asInstanceOf[Msg]).provide(entity))
            ()
            Behaviors.same
          case _ => Behaviors.same
        }
      }
  }

}
