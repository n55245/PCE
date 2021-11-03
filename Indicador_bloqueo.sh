#!/bin/bash

##########################################################################################################################################
																														#CONFIGURACION
##########################################################################################################################################
source /opt/cloudera/parcels/Anaconda3/scripts/python/spark2/anaconda-spark2-env.sh
fecha=`date +%Y%m%d_%H%M%S`
logpath="/home/ad/"${USER}"/SEG/MDC_SEG/logs/Indicador_bloqueo"
drivmem="--driver-memory 8g"
numexec="--num-executors 20"
execmem="--executor-memory 16g"


##########################################################################################################################################
																														#EJECUCION DEL MODELO
##########################################################################################################################################
if [ $# -lt 1 ]; then
	echo "[ERROR] No se ha introducido la fecha de ejecucion."
else
  name="Indicador_bloqueo"
	codigo=${name}".py"
	spark-submit --name ${name} --deploy-mode=client ${drivmem} ${execmem} ${numexec} ${codigo} ${1} 2> ${logpath}/${fecha}"_log_cluster.log"
	appId=`sed -n 's/^.*\(application\)\(.*\)\(\/.*\)/\1\2/p' $logpath/$fecha"_log_cluster.log" | head -n 1`
	yarn logs -applicationId $appId > ${logpath}/${fecha}"_log_ejecucion.log"
	cat ${logpath}/${fecha}"_log_ejecucion.log" | grep -E 'INICIO -|PASO -|FIN -' >> ${logpath}/${fecha}"_log_interno.log"

fi
