-- Script Pig para analizar total de transacciones y valor promedio por ubicación

-- Cargar CSV desde HDFS
data = LOAD '/user/cloudera/massive_bank_project/data_csv/bankdataset_real2.csv' 
    USING PigStorage(',') 
    AS (domain:chararray, location:chararray, value:double, transaction_count:int);

-- Agrupar por ubicación
agrupado = GROUP data BY location;

-- Calcular totales por ubicación
resultados = FOREACH agrupado GENERATE 
    group AS `location`,
    SUM(data.transaction_count) AS total_transacciones,
    SUM(data.value) / SUM(data.transaction_count) AS promedio_valor;

-- Ordenar por número de transacciones descendente
ordenado = ORDER resultados BY total_transacciones DESC;

-- Guardar resultados en HDFS
STORE ordenado INTO '/user/cloudera/massive_bank_project/processed/pig_output' 
    USING PigStorage(',');