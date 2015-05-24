package io.github.morgaroth.akka.stream.statewithcache

import akka.actor.ActorSystem
import akka.stream.{FanInShape2, UniformFanOutShape, ActorFlowMaterializer}
import akka.stream.scaladsl._
import akka.stream.stage.{SyncDirective, Context, StageState, StatefulStage}
import io.github.morgaroth.akka.stream.statewithcache.models.{RootDataRaw, ElemRaw, RootData}

import scala.collection.immutable.IndexedSeq
import scala.concurrent.Future
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

object CachingStream {
  def main(args: Array[String]) {
    implicit val system = ActorSystem("Sys")
    implicit val materializer = ActorFlowMaterializer()

    val g = FlowGraph.closed() { implicit builder: FlowGraph.Builder[Unit] =>
      import FlowGraph.Implicits._
      val source = Source(1 until 100 map (x => RootData.generate))

      val bcast = builder.add(Broadcast[RootData](2))

      val saveElems = Flow[RootData]
        .map(_.elem)
        .map(x => ElemRaw(0, x.data))
        .transform(() => elemsStage)

      val transformRaws = Flow[RootData]
        .map(x => RootDataRaw.apply(0, x.info, 0))

      val zip = builder.add(Zip[Int, RootDataRaw]())

      val updateElemId = Flow[(Int, RootDataRaw)]
        .map(x => x._2.copy(elemId = x._1))

      val saveRaw = Flow[RootDataRaw]
        .map(RootDataRaw.save)

      //@formatter:off
      source ~> bcast ~> saveElems     ~> zip.in0; zip.out ~> updateElemId ~> saveRaw ~> Sink.ignore
                bcast ~> transformRaws ~> zip.in1
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

  //  def elemsStage = new StatefulStage[ElemRaw, Int] {
  //    var idsCache = Map.empty[String, Int]
  //
  //    override def initial: StageState[ElemRaw, Int] = new State {
  //      override def onPush(elem: ElemRaw, ctx: Context[Int]): SyncDirective = {
  //        if (!idsCache.contains(elem.data)) {
  //          idsCache += elem.data -> ElemRaw.save(ElemRaw(0, elem.data)).id
  //        }
  //        emit(List(idsCache(elem.data)).iterator, ctx)
  //      }
  //    }
  //  }
}
