#!/bin/bash

sqoop_import(){
  
 FILE=$1

 data_source=$(grep -i 'dataSource' $FILE  | cut -f2 -d'=')
 user_name=$(grep -i 'userName' $FILE  | cut -f2 -d'=')
 password=$(grep -i 'password' $FILE  | cut -f2 -d'=')
 host_name=$(grep -i 'hostName' $FILE  | cut -f2 -d'=')
 port_number=$(grep -i 'portNumber' $FILE  | cut -f2 -d'=')
 db_name=$(grep -i 'databaseName' $FILE  | cut -f2 -d'=')
 #schema_name=$(grep -i 'schemaName' $FILE  | cut -f2 -d'=')
 #table_name=$(grep -i 'tableName' $FILE  | cut -f2 -d'=')
 #partition_key=$(grep -i 'partitionKey' $FILE  | cut -f2 -d'=')
 hive_db_name=$(grep -i 'hivedb' $FILE  | cut -f2 -d'=')


 echo "Data Source: " $data_source >> ${2}.txt
 echo "User Name: " $user_name >> ${2}.txt
 echo "Host Name: " $host_name >> ${2}.txt
 echo "Port Number: " $port_number >> ${2}.txt
 echo "hivedb" : "$hive_db_name" >> ${2}.txt


 IFS='| ' read -r -a array <<< "$db_name"

 for element in "${array[@]}"
 do
               begin1=$(date +"%s")
               echo "$element" >> ${2}.txt
               db_name="$( echo "$element" | awk -F. '{print $1}')"
               table_name="$( echo "$element" | awk -F. '{print $2}')"
               partition_key="$( echo "$element" | awk -F. '{print $3}')"
               bucket="$( echo "$element" | awk -F. '{print $4}')"
               if [ -z "$hive_db_name" ]; then 
                    hive_db_name=$db_name
               fi
               echo "Database Name: " $db_name >> ${2}.txt
               #echo "Schema Name: " $schema_name
               echo "Table Name : " $table_name >> ${2}.txt
               echo "Partition Key : " $partition_key >> ${2}.txt
               echo "Bucket :" $bucket >> ${2}.txt
               echo "Importing table $db_name.$table_name"
               hive -hiveconf myvar=$hive_db_name -e 'CREATE DATABASE IF NOT EXISTS ${hiveconf:myvar};' 2>> ${2}.log
               if [ -z "$partition_key" ] && [ -z "$bucket" ]; then 
               echo "RDBMS Count For $db_name.$table_name is :" >> ${2}.txt
               sqoop eval --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --query "SELECT count(*) AS MYSQL_COUNT FROM $table_name" 2>> ${2}.log 1>> ${2}.txt 

               sqoop import --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --table $table_name --hive-import --hive-table $hive_db_name.${table_name} --create-hive-table -m4 --validate --validator org.apache.sqoop.validation.RowCountValidator --validation-threshold org.apache.sqoop.validation.AbsoluteValidationThreshold --validation-failurehandler org.apache.sqoop.validation.AbortOnFailureHandler &>> ${2}.log
                if [ $? = 0 ]; then
                 echo "Hive Count For $db_name.$table_name is :" >> ${2}.txt 
                 ht=$hive_db_name.${table_name}
                 nohup hive -hiveconf myvar=$ht -e 'select count(*) from ${hiveconf:myvar};' 2>> ${2}.log 1>> ${2}.txt
                 echo "import for $db_name.$table_name is success" >> ${2}.txt
                else
                 echo "import for $db_name.$table_name is failed" >> ${2}.txt
                 touch failure/${2}_${db_name}_${table_name}
                 echo "appid=${2}" >> failure/${2}_${db_name}_${table_name}
                 filecontent=$(<job_2.properties)
                 echo "$filecontent" >> failure/${2}_${db_name}_${table_name} 
                 sed -i.bak "/databaseName/d" failure/${2}_${db_name}_${table_name}
                 echo "databaseName=$element" >> failure/${2}_${db_name}_${table_name}         
                fi                      
               elif [ -z "$bucket" ] && [ ! -z "$partition_key" ]; then
               flag=0
               echo "RDBMS Count For $db_name.$table_name is :" >> ${2}.txt
               sqoop eval --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --query "SELECT count(*) AS MYSQL_COUNT FROM $table_name" 2>> ${2}.log 1>> ${2}.txt   
               sqoop import --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --table $table_name --where "1=0" --hive-import --hive-table $hive_db_name.${table_name}_temp --create-hive-table -m4 &>> ${2}.log
               if [ $? != 0 ]; then
                flag=1
               fi           
               ht=$hive_db_name.${table_name}_temp
               nohup hive -hiveconf myvar=$ht -e 'desc ${hiveconf:myvar};' 2>> ${2}.log 1>schema.txt
               sed -i.bak "/WARN/d" schema.txt >> ${2}.log
               keyarray=(${partition_key//,/ })
               pstatement=""
               if [ ${#keyarray[@]} -eq 1 ]; then
                    dtype=$(grep -n "$partition_key" schema.txt | awk -F  "\t" '{print $2}' | sed 's/,*$//g')
                    pstatement=$partition_key" "$dtype
                    sed -i.bak "/$partition_key/d" schema.txt >> ${2}.log
               else
                    for i in "${keyarray[@]}"
                    do
                      dtype=$(grep -n "$i" schema.txt | awk -F  "\t" '{print $2}' | sed 's/,*$//g')
                      pstatement=$pstatement$i" "$dtype","
                      sed -i.bak "/$i/d" schema.txt >> ${2}.log
                    done
                    pstatement=${pstatement%?}
               fi
               sed -i.bak 's/[[:blank:]]*$//' schema.txt >> ${2}.log
               sed -i.bak '$!s/$/,/' schema.txt >> ${2}.log
               value=$(<schema.txt)
               echo "CREATE TABLE $hive_db_name.${table_name}
               (
                 $value
               ) PARTITIONED BY ($pstatement) row format delimited
               fields terminated by ','
               stored as textfile;" > script.hql
               hive -f script.hql 2>> ${2}.log
               if [ $? != 0 ]; then
                flag=1
               fi
               sqoop import --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --table $table_name --hcatalog-database $hive_db_name --hcatalog-table $table_name -m4 --validate --validator org.apache.sqoop.validation.RowCountValidator --validation-threshold org.apache.sqoop.validation.AbsoluteValidationThreshold --validation-failurehandler org.apache.sqoop.validation.AbortOnFailureHandler &>> ${2}.log
               if [ $? != 0 ] || [ $flag != 0 ]; then
                 echo "import for $db_name.$table_name is failed" >> ${2}.txt
                 touch failure/${2}_${db_name}_${table_name}
                 echo "appid=${2}" >> failure/${2}_${db_name}_${table_name}
                 filecontent=$(<job_2.properties)
                 echo "$filecontent" >> failure/${2}_${db_name}_${table_name}
                 sed -i.bak "/databaseName/d" failure/${2}_${db_name}_${table_name}
                 echo "databaseName=$element" >> failure/${2}_${db_name}_${table_name} 
               else
                 echo "Hive Count For $db_name.$table_name is :" >> ${2}.txt                 
                 ht=$hive_db_name.${table_name}
                 nohup hive -hiveconf myvar=$ht -e 'select count(*) from ${hiveconf:myvar};' 2>> ${2}.log 1>> ${2}.txt
                 echo "import for $db_name.$table_name is success" >> ${2}.txt
               fi
               ht=$hive_db_name.${table_name}_temp                             
               hive -hiveconf myvar=$ht -e 'drop table if exists ${hiveconf:myvar};' 2>> ${2}.log
               elif [ -z "$partition_key" ] && [ ! -z "$bucket" ]; then
               flag=0
               bucket_key="$( echo "$bucket" | awk -F: '{print $1}')"
               bucket_num="$( echo "$bucket" | awk -F: '{print $2}')"
               echo "Bucket Key : " $bucket_key >> ${2}.txt
               echo "bucket_num : " $bucket_num >> ${2}.txt
               echo "RDBMS Count For $db_name.$table_name is :" >> ${2}.txt
               sqoop eval --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --query "SELECT count(*) AS MYSQL_COUNT FROM $table_name" 2>> ${2}.log 1>> ${2}.txt
               sqoop import --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --table $table_name --hive-import --hive-table $hive_db_name.${table_name}_temp --create-hive-table -m4 --validate --validator org.apache.sqoop.validation.RowCountValidator --validation-threshold org.apache.sqoop.validation.AbsoluteValidationThreshold --validation-failurehandler org.apache.sqoop.validation.AbortOnFailureHandler &>> ${2}.log
               if [ $? != 0 ]; then
                flag=1
               fi
               keyarray=(${bucket_key//,/ })
               pstatement=""
               if [ ${#keyarray[@]} -eq 1 ]; then
                pstatement=$bucket_key
               else
                for i in "${keyarray[@]}"
                do
                  pstatement=$pstatement$i","
                done
                pstatement=${pstatement%?}
               fi
               ht=$hive_db_name.${table_name}_temp
               nohup hive -hiveconf myvar=$ht -e 'desc ${hiveconf:myvar};' 2>> ${2}.log 1>schema.txt
               sed -i.bak "/WARN/d" schema.txt >> ${2}.log
               sed -i.bak 's/[[:blank:]]*$//' schema.txt >> ${2}.log
               sed -i.bak '$!s/$/,/' schema.txt >> ${2}.log
               value=$(<schema.txt)
               echo "CREATE TABLE $hive_db_name.${table_name}
               (
                 $value
               ) CLUSTERED BY ($pstatement) INTO $bucket_num buckets 
               row format delimited
               fields terminated by ','
               stored as textfile; set hive.enforce.bucketing = true; 
               INSERT INTO TABLE $hive_db_name.${table_name} select * from $hive_db_name.${table_name}_temp;" > script.hql
               hive -f script.hql 2>> ${2}.log
               if [ $? != 0 ] || [ $flag != 0 ]; then
                 echo "import for $db_name.$table_name is failed" >> ${2}.txt
                 touch failure/${2}_${db_name}_${table_name}
                 echo "appid=${2}" >> failure/${2}_${db_name}_${table_name}
                 filecontent=$(<job_2.properties)
                 echo "$filecontent" >> failure/${2}_${db_name}_${table_name}
                 sed -i.bak "/databaseName/d" failure/${2}_${db_name}_${table_name}
                 echo "databaseName=$element" >> failure/${2}_${db_name}_${table_name}
               else
                 echo "Hive Count For $db_name.$table_name is :" >> ${2}.txt                 
                 ht=$hive_db_name.${table_name}
                 nohup hive -hiveconf myvar=$ht -e 'select count(*) from ${hiveconf:myvar};' 2>> ${2}.log 1>> ${2}.txt
                 echo "import for $db_name.$table_name is success" >> ${2}.txt
               fi 
               ht=$hive_db_name.${table_name}_temp
               hive -hiveconf myvar=$ht -e 'drop table ${hiveconf:myvar};' 2>> ${2}.log
                       
               else
               flag=0
               bucket_key="$( echo "$bucket" | awk -F: '{print $1}')" 
               bucket_num="$( echo "$bucket" | awk -F: '{print $2}')"
               echo "Bucket Key : " $bucket_key >> ${2}.txt
               echo "bucket_num : " $bucket_num >> ${2}.txt
               echo "RDBMS Count For $db_name.$table_name is :" >> ${2}.txt
               sqoop eval --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --query "SELECT count(*) AS MYSQL_COUNT FROM $table_name" 2>> ${2}.log 1>> ${2}.txt
               sqoop import --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --table $table_name --where "1=0" --hive-import --hive-table $hive_db_name.${table_name}_temp1 --create-hive-table -m4 &>> ${2}.log
               if [ $? != 0 ]; then
                flag=1
               fi
               ht=$hive_db_name.${table_name}_temp1
               nohup hive -hiveconf myvar=$ht -e 'desc ${hiveconf:myvar};' 2>> ${2}.log 1>schema.txt
               sed -i.bak "/WARN/d" schema.txt >> ${2}.log
               keyarray=(${partition_key//,/ })
               pstatement=""
               bstatement=""
               if [ ${#keyarray[@]} -eq 1 ]; then
                    dtype=$(grep -n "$partition_key" schema.txt | awk -F  "\t" '{print $2}' | sed 's/,*$//g')
                    pstatement=$partition_key" "$dtype
                    bstatement=$partition_key
                    sed -i.bak "/$partition_key/d" schema.txt >> ${2}.log
               else
                    for i in "${keyarray[@]}"
                    do
                      dtype=$(grep -n "$i" schema.txt | awk -F  "\t" '{print $2}' | sed 's/,*$//g')
                      pstatement=$pstatement$i" "$dtype","
                      bstatement=$bstatement$i","
                      sed -i.bak "/$i/d" schema.txt >> ${2}.log
                    done
                    pstatement=${pstatement%?}
                    bstatement=${bstatement%?}                  
               fi
               sed -i.bak 's/[[:blank:]]*$//' schema.txt >> ${2}.log
               sed -i.bak '$!s/$/,/' schema.txt >> ${2}.log
               value=$(<schema.txt)
               echo "CREATE TABLE $hive_db_name.${table_name}_temp2
               (
                 $value
               ) PARTITIONED BY ($pstatement) row format delimited
               fields terminated by ','
               stored as textfile;" > script.hql
               hive -f script.hql 2>> ${2}.log
               if [ $? != 0 ]; then
                flag=1
               fi 
               sqoop import --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --table $table_name --hcatalog-database $hive_db_name --hcatalog-table ${table_name}_temp2 -m4 --validate --validator org.apache.sqoop.validation.RowCountValidator --validation-threshold org.apache.sqoop.validation.AbsoluteValidationThreshold --validation-failurehandler org.apache.sqoop.validation.AbortOnFailureHandler &>> ${2}.log
               if [ $? != 0 ]; then
                flag=1
               fi 
               echo "CREATE TABLE $hive_db_name.${table_name}
               (
                 $value
               ) PARTITIONED BY ($pstatement) CLUSTERED BY ($bucket_key) INTO $bucket_num buckets row format delimited
               fields terminated by ','
               stored as textfile; set hive.enforce.bucketing = true; set hive.exec.dynamic.partition.mode=nonstrict; 
               INSERT INTO TABLE $hive_db_name.${table_name} partition($bstatement) select * from $hive_db_name.${table_name}_temp2;
               " > script.hql
               hive -f script.hql 2>> ${2}.log
               if [ $? != 0 ] || [ $flag != 0 ]; then
                 echo "import for $db_name.$table_name is failed" >> ${2}.txt
                 touch failure/${2}_${db_name}_${table_name}
                 echo "appid=${2}" >> failure/${2}_${db_name}_${table_name}
                 filecontent=$(<job_2.properties)
                 echo "$filecontent" >> failure/${2}_${db_name}_${table_name}
                 sed -i.bak "/databaseName/d" failure/${2}_${db_name}_${table_name}
                 echo "databaseName=$element" >> failure/${2}_${db_name}_${table_name}
               else
                 echo "Hive Count For $db_name.$table_name is :" >> ${2}.txt                 
                 ht=$hive_db_name.${table_name}
                 nohup hive -hiveconf myvar=$ht -e 'select count(*) from ${hiveconf:myvar};' 2>> ${2}.log 1>> ${2}.txt
                 echo "import for $db_name.$table_name is success" >> ${2}.txt
               fi
               ht=$hive_db_name.${table_name}_temp1
               hive -hiveconf myvar=$ht -e 'drop table ${hiveconf:myvar};' 2>> ${2}.log
               ht=$hive_db_name.${table_name}_temp2
               hive -hiveconf myvar=$ht -e 'drop table ${hiveconf:myvar};' 2>> ${2}.log
               fi           

  echo "Import completed for table $db_name.$table_name"
termin1=$(date +"%s")
difftime1=$(($termin1-$begin1))
minutes1=$((difftime1 / 60))
seconds1=$(($difftime1 % 60))
echo "Total Time for Execution of table $db_name.$table_name : $minutes1 minutes $seconds1 seconds"
echo "Total Time for Execution of table $db_name.$table_name : $minutes1 minutes $seconds1 seconds" >> ${2}.txt
echo "-----------------------------------------------------------------------------------------" >> ${2}.txt
echo "-----------------------------------------------------------------------------------------" >> ${2}.log
done
}

appid(){
v=$(cat $1/sequence.txt)
today="$( echo "$v" | awk -F: '{print $1}')"
seq="$( echo "$v" | awk -F: '{print $2}')"
todate=$(date -d $today +"%Y%m%d")
conditi=$(date +'%Y%m%d')
aid=""
if [ $todate -eq $conditi ]; then
 aid="application-$(date '+%Y%m%d%H%M%S')-$seq"
 seq=$((seq+1))
 echo "$today:$seq" > $1/sequence.txt
else
 echo "$(date +"%F"):1" > $1/sequence.txt
 v=$(cat $1/sequence.txt)
 today="$( echo "$v" | awk -F: '{print $1}')"
 seq="$( echo "$v" | awk -F: '{print $2}')"
 aid="application-$(date '+%Y%m%d%H%M%S')-$seq"
 seq=$((seq+1))
 echo "$today:$seq" > $1/sequence.txt
fi
#return "$aid"
}

echo "----------------------IFW Application---------------------------"
aid=""
echo "Application Started"
begin=$(date +"%s")
x1=${1%/*}

if [ $(ls $x1/failure | wc -l) -ge 1 ]; then
 #source ./appid.bash
 appid $x1
               fi
 echo "Application ID: $aid"
 echo "In Progress..."
 echo "Application Started" >> ${aid}.log
 echo "Application ID: $aid" >> ${aid}.log
 echo "Error: there are $(ls $x1/failure | wc -l) files in failure folder please move file from failure dir to tobeprocessed dir and again run the script" >> ${aid}.lo
g
 
elif [ $(ls $x1/tobeprocessed | wc -l) -ge 1 ]; then
  for FILE in $x1/tobeprocessed/*; do
  aid=$(grep -i 'appid' $FILE  | cut -f2 -d'=')
  echo "Application ID: $aid"
  echo "In Progress..."
  echo "Application Started" >> ${aid}.log
  echo "Application ID: $aid" >> ${aid}.log
  sqoop_import "$FILE" "$aid" 2>> ${aid}.log
  rm $FILE >> ${aid}.log
 done
else
 #source ./appid.bash
 appid $x1
 echo "Application ID: $aid"
 echo "Import In Progress..."
 echo "Application Started" >> ${aid}.log
 echo "Application ID: $aid" >> ${aid}.log
 FILE=$1
 sqoop_import "$FILE" "$aid" 2>> ${aid}.log
fi
termin=$(date +"%s")
difftime=$(($termin-$begin))
minutes=$((difftime / 60))
seconds=$(($difftime % 60))
echo "Total Time for Execution: $minutes minutes $seconds seconds"
echo "Total Time for Execution: $minutes minutes $seconds seconds" >> ${aid}.log
echo "Import Completed"
echo "-----------------------------END---------------------------------"
echo "Application Ended" >> ${aid}.log
echo "Please check ${aid}.log for more info"
