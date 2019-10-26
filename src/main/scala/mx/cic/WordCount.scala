package mx.cic
//Librerias
import org.apache.flink.api.scala._
import java.io.File
import java.io._
import scala.collection.mutable.ListBuffer
// Clase principal
object WordCount {
  def main(args: Array[String]) {
    //val env = ExecutionEnvironment.getExecutionEnvironment
    // Definimos env como variable para la lectura del archivo
    val env = ExecutionEnvironment.getExecutionEnvironment
    // Direccion de los argumentos, en este caso del archivo
    val direccion = args(0)
    // Creo listas donde guardare lo que requiero
    val  totales= new ListBuffer[String]()
    val contenido = new ListBuffer[String]()
    var lista_de_contenido = new ListBuffer[String]()
    var nombres = new ListBuffer[String]()
    // files sera la lista donde la funcion getOfListFiles me dara los nombres de los archivos que tengo en mi carpeta llamada prueba
    val files = getListOfFiles(direccion)
    println(files)

    // Valor que le doy a la función que me imprime en mi archivo de salida llamado output.txt
    val writer = new PrintWriter(new File("output.txt"))
    //Creo mi fun1
    def fun1(x: File) = {
      //println("veces")
      //Leo el archivo segun el argumento de fun1
      val text = env.readTextFile(x.toString)
      //Guardamos el nombre del archivo en la lista nombres
      nombres += x.toString
      totales += x.toString
      // counts sera el valor designaod para (palabra,#repeticion)
      val counts = text.flatMap {
        _.toLowerCase.split("\\W+")
      }
        .map {
          (_, 1)
        }
        .groupBy(0)
        .sum(1)
      //al archivo sera coounts.collect
      val alarchivo = counts.collect()
      //val contenido = new ListBuffer[String]()
      contenido.clear()
      for (x <- alarchivo) {
        //guardo en la lista contenido

        contenido += x.toString
       // val cont = List[String](x.toString())
      }

      // suma es para el numero total de palabras en el documento
      val suma = text.flatMap {
        _.toLowerCase.split("\\W+")
      }.map {
        (_, 1)
      }.count() // Obtenemos la suma del total de las palabras
      //Guardo en la lista totales
      totales += suma.toInt.toString
    }

    // hago un for e itero la fun1 para cada archivo en la lista files

    for (x1 <- files) {
      fun1(x1)
      //val result = fun1(x1)
      //println(contenido)
      lista_de_contenido +=  contenido.toString()
    }
    println(nombres)
    //println(lista_de_contenido)
    //println(lista_de_contenido(0))
    //println(lista_de_contenido(1))
    val arr = "***************"
    // Escribo en el documento de salida las listas que cree
    writer.write("Este es el documento de salida")
    writer.write("\n")
    for (x <- nombres) {
      writer.write(x)
      writer.write("\n")
    }
    writer.write(arr)
    writer.write("\n")
    for (x <- totales) {
      writer.write("\n")
      writer.write(x)
      writer.write("\n")
    }

    writer.write(arr)
    writer.write("\n")

    var ii = 0
    for (x <- lista_de_contenido) {
      ii +=  1
      writer.write("\n")
      writer.write(nombres(ii))
      writer.write("\n")
      for (y<-x) writer.write(y)
    }
    writer.write("\n")


    writer.close()
  }
  // Función con la que extraigo los nombres de los archivos.txt
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}
