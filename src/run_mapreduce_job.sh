#!/bin/bash

# Compilar clase Java
javac -classpath `hadoop classpath` -d . ValorPorDominio.java

# Crear JAR
jar -cvf valordominio.jar *.class

# Ejecutar el job en Hadoop
hadoop jar valordominio.jar ValorPorDominio \
/user/cloudera/massive_bank_project/data_csv/bankdataset_real2.csv \
/user/cloudera/massive_bank_project/processed/mapreduce_output