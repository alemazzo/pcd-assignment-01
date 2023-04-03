import ProducerConsumer.{Consumer, Producer, QueueConsumer, QueueConsumerThread, QueueProducer}
import Synchronization.Monitor
import Utils.{Closeable, CloseableQueue, QueueMonitor}

import java.lang.System.currentTimeMillis
import java.nio.file.{Files, Path}
import java.util.concurrent.locks.{Condition, ReentrantLock}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import collection.mutable.Queue
import scala.collection.mutable
import scala.io.Source
import scala.io.Source.fromFile

object Synchronization:
  trait Monitor:
    private val mutex = new ReentrantLock()
    protected def newCondition(): Condition = mutex.newCondition()
    protected def monitored[A](f: => A): A =
      mutex.lock()
      val res = f
      mutex.unlock()
      res
end Synchronization

object Queues:
  trait Enqueuable[T]:
    def enqueue(elem: T): Unit
  trait Dequeuable[T]:
    def dequeue(): Option[T]
  trait Queue[T] extends Enqueuable[T] with Dequeuable[T]
end Queues

object Utils:
  import Queues.Queue
  trait Closeable:
    def close(): Unit
    def isOpen: Boolean
  trait CloseableQueue[T] extends Queue[T] with Closeable
  class QueueMonitor[T](private val queue: mutable.Queue[T] = mutable.Queue.empty[T]) extends Monitor with CloseableQueue[T]:
    private val notEmpty = newCondition()
    private var open = true

    override def enqueue(elem: T): Unit = monitored {
      queue.enqueue(elem)
      notEmpty.signal()
    }
    override def dequeue(): Option[T] = monitored {
      while queue.isEmpty && open do
        notEmpty.await()
      if queue.nonEmpty then Some(queue.dequeue()) else None
    }

    override def close(): Unit = monitored {
      open = false
      notEmpty.signalAll()
    }
    override def isOpen: Boolean = monitored {
      open || queue.nonEmpty
    }
  end QueueMonitor
end Utils

object ProducerConsumer:
  import Utils.CloseableQueue
  trait Producer[T]:
    def produce(t: T): Unit
  trait Consumer[T]:
    def consume(t: T): Unit

  trait QueueProducer[T](private val queue: QueueMonitor[T]) extends Producer[T]:
    override def produce(t: T): Unit = queue.enqueue(t)
    def complete(): Unit = queue.close()

  trait QueueConsumer[T] extends Consumer[T]:
    def onCompleted(): Unit

  trait QueueConsumerThread[T](private val queue: QueueMonitor[T]) extends Thread with QueueConsumer[T]:
    final override def run(): Unit =
      while queue.isOpen do
        queue.dequeue() match
          case Some(t) => consume(t)
          case None => ()
      onCompleted()
end ProducerConsumer

object Assignment:
  case class Stats(file: Path, lines: Int)
  case class StatsConfiguration(n: Int)
  class FileQueue extends QueueMonitor[Path]()
  class StatsQueue extends QueueMonitor[Stats]()

  class StatsMonitor extends Monitor:
    var totalLines = 0
    def addLines(lines: Int): Unit = monitored { totalLines += lines }

  class FilesProducer(root: Path, fileQueue: FileQueue) extends Thread with QueueProducer[Path](fileQueue):
    override def run(): Unit =
      val files = Files.walk(root).filter(_.toString.endsWith(".java"))
      files.forEach(produce)
      complete()

  class FilesConsumer(fileQueue: FileQueue, statsQueue: StatsQueue) extends QueueConsumerThread[Path](fileQueue) with QueueProducer[Stats](statsQueue):
    override def onCompleted(): Unit = complete()
    override def consume(file: Path): Unit =
      try
        val lines = Files.readAllLines(file).size()
        produce(Stats(file, lines))
      catch
        case e: Exception => println(s"Error while processing $file: $e")

  class StatsConsumer(config: StatsConfiguration, statsQueue: StatsQueue, statsMonitor: StatsMonitor) extends QueueConsumerThread[Stats](statsQueue):
    override def onCompleted(): Unit = println("Stats consumer completed")
    override def consume(stats: Stats): Unit =
      statsMonitor.addLines(stats.lines)
      //println(s"File ${stats.file} has ${stats.lines} lines")
end Assignment

object Main:
  import Queue.*
  import ProducerConsumer.*
  import Assignment.*
  def main(args: Array[String]): Unit = {
    val nFileConsumers = 10
    val nStatsConsumer = 10
    val statsConfiguration = StatsConfiguration(n = 10)
    val pcdFolder = "/home/alessandro/Desktop/Università"

    val fileQueue = FileQueue()
    val statsQueue = StatsQueue()
    val statsMonitor = StatsMonitor()

    val fileProducer = FilesProducer(Path.of(pcdFolder), fileQueue)
    val fileConsumers = (1 to nFileConsumers) map (_ => FilesConsumer(fileQueue, statsQueue))
    val statsConsumers = (1 to nStatsConsumer) map (_ => StatsConsumer(statsConfiguration, statsQueue, statsMonitor))

    statsConsumers.foreach(_.start())
    fileConsumers.foreach(_.start())
    fileProducer.start()
    // Save current millis
    val start = currentTimeMillis()
    fileProducer.join()
    println("File producer done")
    fileConsumers.foreach(_.join())
    println("File consumers done")
    statsConsumers.foreach(_.join())
    println("Stats consumers done")
    val end = currentTimeMillis()

    println(s"Done in ${end - start} ms")
    println(s"Total lines: ${statsMonitor.totalLines}")
  }