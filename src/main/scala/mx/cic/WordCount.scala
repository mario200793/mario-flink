package mx.cic

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 //Librerias
import org.apache.flink.api.scala._
import java.io.File
import java.io._
import scala.collection.mutable.ListBuffer

// Clase principal
object WordCount {
  def main(args: Array[String]) {

    // set up the execution environment
    //val env = ExecutionEnvironment.getExecutionEnvironment
    // Definimos env como variable para la lectura del archivo
    val env = ExecutionEnvironment.getExecutionEnvironment
    // Direccion de los argumentos, en este caso del archivo
    val direccion = args(0)
    // Creo listas donde guardare lo que requiero
    val  totales= new ListBuffer[String]()
    val contenido = new ListBuffer[String]()
    //val lista_de_contenido = List[List[String]]
    var nombres = new ListBuffer[String]()
    // files sera la lista donde la funcion getOfListFiles me dara los nombres de los archivos que tengo en mi carpeta llamada prueba
    val files = getListOfFiles(direccion)

    // Valor que le doy a la función que me imprime en mi archivo de salida llamado output.txt
    val writer = new PrintWriter(new File("output.txt"))
    //Creo mi fun1
    def fun1(x: File) = {
      //Leo el archivo segun el argumento de fun1
        val text = env.readTextFile(x.toString)
        println("El archivo que estamos eyendo es: ")
      //Escribo en el documento
        //writer.write("El archivo que estamos leyendo es:")
        //writer.write("\n")
        //writer.write(x.toString)
      //Guardamos el nombre del archivo en la lista nombres
        nombres += x.toString
        totales += x.toString
        //writer.write("\n")
        println(x)
      // counts sera el valor designaod para (palabra,#repeticion)
        val counts = text.flatMap {
          _.toLowerCase.split("\\W+")
        }
          .map {
            (_, 1)
          }
          .groupBy(0)
          .sum(1)
        println("Las palabras que encontramos y las veces que se repiten en este archivo son: ")
        //writer.write("Las palabras que encontramos y las veces que se repiten en este archivo son: ")
        //writer.write("\n")
      //al archivo sera coounts.collect
      val alarchivo = counts.collect()
      for (x <- alarchivo) {
        //guardo en la lista contenido
        contenido += x.toString
          //writer.write(x.toString())
          //writer.write("\n")
        }
        //writer.write("\n")
        counts.print()
      // suma es para el numero total de palabras en el documento
      val suma = text.flatMap {
          _.toLowerCase.split("\\W+")
        }.map {
          (_, 1)
        }.count() // Obtenemos la suma del total de las palabras
        println("El # de palabras de este archivo son: ")
        //writer.write("El # de palabras de este archivo son: ")
        //writer.write("\n")
      //Guardo en la lista totales
        totales += suma.toInt.toString

        //writer.write(suma.toInt.toString)
        //writer.write("\n")
        println(suma.toInt)
      }

    // hago un for e itero la fun1 para cada archivo en la lista files
    for (x1 <- files) {
      fun1(x1)
      //lista_de_contenido = contenido.toList

    }
    val arr = "***************"
  // Escribo en el documento de salida las listas que cree
    for (x <- nombres) {
      writer.write(x)
      writer.write("\n")
    }
    writer.write(arr)
    for (x <- totales) {
      //writer.write("El total para el documento ")
      writer.write("\n")
      writer.write(x)
      writer.write("\n")
    }

    writer.write(arr)
    writer.write("\n")

    for (x <- nombres){
      writer.write("\n")
      writer.write(x)
      writer.write("\n")
      for (y <- contenido) {
        writer.write(y)
        writer.write("\n")
      }
      }
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

