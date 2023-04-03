import Assignment.QueueMonitor
import ProducerConsumer.{Consumer, ConsumerThread, Producer}
import Queue.CloseableQueue

import java.nio.file.{Files, Path}
import java.util.concurrent.locks.{Condition, ReentrantLock}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import collection.mutable.Queue
import scala.collection.mutable
import scala.io.Source
import scala.io.Source.fromFile

trait Monitor:
  private val mutex = new ReentrantLock()
  protected def newCondition(): Condition = mutex.newCondition()
  protected def monitored[A](f: => A): A =
    mutex.lock()
    val res = f
    mutex.unlock()
    res

object Queue:
  trait Enqueuable[T]:
    def enqueue(elem: T): Unit
  trait Dequeuable[T]:
    def dequeue(): Option[T]
  trait Queue[T] extends Enqueuable[T] with Dequeuable[T]

  trait Closeable:
    def close(): Unit
    def isOpen: Boolean
  trait CloseableQueue[T] extends Queue[T] with Closeable
end Queue

object ProducerConsumer:
  import Queue.*
  trait Producer[T](queue: CloseableQueue[T]):
    final def produce(t: T): Unit = queue.enqueue(t)

  trait Consumer[T]:
    protected def consume(t: T): Unit
    protected def onCompleted(): Unit = ()

  trait ConsumerThread[T](queue: CloseableQueue[T]) extends Thread with Consumer[T]:
    final override def run(): Unit =
      while queue.isOpen do
        queue.dequeue() match
          case Some(t) => consume(t)
          case None => ()
      onCompleted()
end ProducerConsumer

object Assignment:
  import Queue.*
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

  case class Stats(file: Path, lines: Int)
  case class StatsConfiguration(n: Int)
  class FileQueue extends QueueMonitor[Path]()
  class StatsQueue extends QueueMonitor[Stats]()

  class StatsMonitor:
    var totalLines = 0
    def addLines(lines: Int): Unit = synchronized { totalLines += lines }

  class FilesProducer(root: Path, queue: CloseableQueue[Path]) extends Thread with Producer[Path](queue):
    override def run(): Unit =
      val files = Files.walk(root).filter(_.toString.endsWith(".java"))
      files.forEach(produce)
      queue.close()

  class FilesConsumer(queue: CloseableQueue[Path], stats: StatsQueue) extends ConsumerThread[Path](queue) with Producer[Stats](stats):
    override def onCompleted(): Unit = stats.close()
    override def consume(file: Path): Unit =
      try
        val source = fromFile(file.toFile)
        val lines = source.getLines().size
        produce(Stats(file, lines))
        source.close()
      catch
        case e: Exception => println(s"Error while processing $file: $e")

  class StatsConsumer(config: StatsConfiguration, queue: CloseableQueue[Stats], statsMonitor: StatsMonitor) extends ConsumerThread[Stats](queue):
    override def consume(stats: Stats): Unit =
      statsMonitor.addLines(stats.lines)
      println(s"File ${stats.file} has ${stats.lines} lines")

object Main:
  import Queue.*
  import ProducerConsumer.*
  import Assignment.*
  def main(args: Array[String]): Unit = {
    val nFileConsumers = 10
    val nStatsConsumer = 5
    val statsConfiguration = StatsConfiguration(n = 10)
    val pcdFolder = "/home/alessandro/Desktop/Università/PCD"

    val fileQueue = FileQueue()
    val statsQueue = StatsQueue()
    val statsMonitor = StatsMonitor()

    val fileProducer = FilesProducer(Path.of(pcdFolder), fileQueue)
    val fileConsumers = (1 to nFileConsumers) map (_ => FilesConsumer(fileQueue, statsQueue))
    val statsConsumers = (1 to nStatsConsumer) map (_ => StatsConsumer(statsConfiguration, statsQueue, statsMonitor))

    statsConsumers.foreach(_.start())
    fileConsumers.foreach(_.start())
    fileProducer.start()

    fileProducer.join()
    println("File producer done")
    fileConsumers.foreach(_.join())
    println("File consumers done")
    statsConsumers.foreach(_.join())
    println("Stats consumers done")

    println("Done")
    println(s"Total lines: ${statsMonitor.totalLines}")
  }