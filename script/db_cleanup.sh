#!/bin/bash
DATA_INTERVAL_START=$1
#echo "Configurations:"
#echo "${DATA_INTERVAL_START}"
dateformat=$(date -d ${DATA_INTERVAL_START})
#echo "${dateformat}"
two_days_ago=$(date -d "$dateformat - 41 hours" +"%FT%H:%M:%S")
#echo "${two_days_ago}"

echo "Running Cleanup Process for records before ${two_days_ago} ..."
/home/airflow/.local/bin/airflow db clean --clean-before-timestamp ${two_days_ago} --skip-archive -t 'log','xcom','dag','task_instance','sla_miss','dag_run','task_reschedule','task_fail','import_error','task_instance','celery_taskmeta','dataset_event','job','session','celery_tasksetmeta','rendered_task_instance_fields' --yes

echo "Finished Running Cleanup Process"