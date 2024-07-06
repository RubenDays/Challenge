package challenge

import java.nio.file.{Files, Paths}
import java.util.ResourceBundle
import java.io.File
import scala.reflect.io.Directory

object Funcs {

  private var props: Option[ResourceBundle] = None

  def getOrCreateProps(): ResourceBundle = {
    if (props.isEmpty) {
      props = Option(ResourceBundle.getBundle("props"))
    }
    props.get
  }

  def getFiles(dir: String): Array[String] =
    new File(dir).listFiles.map(p => p.getName)

  def getCsvFile(dir: String): String =
    getFile(dir, ".csv")

  def getFile(dir: String, extension: String): String =
    getFiles(dir).filter(fn => fn.endsWith(extension)).apply(0)

  def renameFile(path: String, tmpPath: String, extension: String): Unit = {
    // get csv filename
    val csvFn = Funcs.getFile(tmpPath, extension)
    val oldFn = tmpPath+"/"+csvFn

    // delete existing old csv file, if exists
    val newFile = new File(path+extension)
    newFile.delete

    // rename the resulting csv file
    new File(oldFn).renameTo(newFile)
    // delete the created directory and the remaining files
    new Directory(new File(tmpPath)).deleteRecursively
  }

}
