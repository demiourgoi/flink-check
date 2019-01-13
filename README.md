# TFM
Random testing basado en lógica temporal para Apache Flink

Herramienta que permite hacer uso de fórmulas de lógica temporal y generadores de ventanas con datos aleatorios para
realizar random testing en aplicaciones de Apache Flink. 

Las clases desarrolladas se encuentran en:
https://github.com/Valcev/TFM/tree/master/polucion/src/main/scala/org


## Ejecutable
Ejecutable disponible en: https://github.com/Valcev/TFM/tree/master/polucion/Ejecutable

**Es necesario tener instalada una versión de Java 1.8 o superior.**

Para ejecutar el archivo 'tfm.jar':

1)Abrir línea de comandos en la carpeta donde se encuentre el archivo 'tfm.jar'. 

2)Introducir cualquiera de los siguientes comandos:

```bash
  java -jar tfm.jar -h
  java -jar tfm.jar -t Pollution
  java -jar tfm.jar -t Race
  java -jar tfm.jar -t KeyedStreamTest
  java -jar tfm.jar -t FormulaTrigger
  java -jar tfm.jar -t PollutionFlinkApp
  java -jar tfm.jar -t StreamTest
  ```
NOTA: Para detener la ejecucion de PollutionFlinkApp y StreamTest es necesario utilizar 'Ctrl + C'.
