package io.github.morgaroth.akka.stream.statewithcache

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage.{SyncDirective, Context, StageState, StatefulStage}
import _root_.io.github.morgaroth.akka.stream.statewithcache.models.{RootDataRaw, ElemRaw, RootData}

import scala.collection.immutable.IndexedSeq
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.Random

object models {

  case class Elem(data: String)

  case class ElemRaw(id: Int, data: String)

  object ElemRaw {
    var id = 0

    def save(elemRaw: ElemRaw) = {
      id += 1
      val copy: ElemRaw = elemRaw.copy(id = id - 1)
      println(s"saved $copy")
      copy
    }
  }

  object Elem {
    val datas: List[String] = 1 to 10 map (x => Random.alphanumeric.take(3).mkString) toList

    def generate = Elem(datas(Random.nextInt(datas.size)))
  }

  case class RootData(info: String, elem: Elem)

  case class RootDataRaw(id: Int, info: String, elemId: Int)

  object RootDataRaw {
    var id = 0

    def save(d: RootDataRaw) = {
      id += 1
      val copy: RootDataRaw = d.copy(id = id - 1)
      println(s"saved $copy")
      copy
    }
  }

  object RootData {
    def generate = RootData(Random.alphanumeric.take(5).mkString, Elem.generate)
  }

}

object MyImplicits {

  import FlowGraph.Implicits._

  implicit def unwrap_out_call[A, B, C](in: FanInShape2[A, B, C]): PortOps[C, Unit] = port2flow(in.out)

  implicit def unwrap_in0_call[A, B, C](in: FanInShape2[A, B, C]): Inlet[A] = in.in0

  implicit def unwrap_in1_call[A, B, C](in: FanInShape2[A, B, C]): Inlet[B] = in.in1
}

object CachingStream {

  def main(args: Array[String]) {
    implicit val system = ActorSystem("Sys")
    implicit val materializer = ActorFlowMaterializer()

    val g = FlowGraph.closed() { implicit builder: FlowGraph.Builder[Unit] =>
      import FlowGraph.Implicits._
      import MyImplicits._
      val source = Source(1 until 100 map (x => RootData.generate))

      val bcast = builder.add(Broadcast[RootData](2))

      val saveElem = Flow[RootData]
        .map(_.elem)
        .map(x => ElemRaw(0, x.data))
        .transform(() => elemsStage)

      val transformRaw = Flow[RootData]
        .map(x => RootDataRaw.apply(0, x.info, 0))

      val zip: FanInShape2[Int, RootDataRaw, (Int, RootDataRaw)] = builder.add(Zip[Int, RootDataRaw]())

      val updateElemId = Flow[(Int, RootDataRaw)]
        .map(x => x._2.copy(elemId = x._1))

      val saveRaw = Flow[RootDataRaw]
        .map(RootDataRaw.save)

      //@formatter:off
      source ~> bcast ~> saveElem     ~> zip ; zip ~> updateElemId ~> saveRaw ~> Sink.ignore
                bcast ~> transformRaw ~> zip
      //@formatter:on
    }
    g.run()
  }

  // this is a bottleneck! may be fast thanks to cache, but doesn't have to... in any way it is still bottleneck :/
  def genericIdElemsCache[ElemType, IdValue, CacheValue](getId: ElemType => IdValue)(calcCacheValue: ElemType => CacheValue) = new StatefulStage[ElemType, CacheValue] {
    var idsCache = Map.empty[IdValue, CacheValue]

    override def initial: StageState[ElemType, CacheValue] = new State {
      override def onPush(elem: ElemType, ctx: Context[CacheValue]): SyncDirective = {
        val id: IdValue = getId(elem)
        if (!idsCache.contains(id)) {
          idsCache += id -> calcCacheValue(elem)
        }
        emit(List(idsCache(id)).iterator, ctx)
      }
    }
  }

  def elemsStage = genericIdElemsCache((e: ElemRaw) => e.data)(e => ElemRaw.save(ElemRaw(0, e.data)).id)
}
