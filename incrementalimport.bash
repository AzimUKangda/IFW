sqoop_incremental_import(){
      FILE=$1
      data_source=$(grep -i 'dataSource' $FILE  | cut -f2 -d'=')
      user_name=$(grep -i 'userName' $FILE  | cut -f2 -d'=')
      password=$(grep -i 'password' $FILE  | cut -f2 -d'=')
      host_name=$(grep -i 'hostName' $FILE  | cut -f2 -d'=')
      port_number=$(grep -i 'portNumber' $FILE  | cut -f2 -d'=')
      db_name=$(grep -i 'mysql_db_tb_check_col' $FILE  | cut -f2 -d'=')
      hive_db_name=$(grep -i 'hivedb' $FILE  | cut -f2 -d'=')
      #hive_tb_name=$(grep -i 'hive_tb' $FILE  | cut -f2 -d'=')
      echo "Data Source: " $data_source >> ${2}.txt
      echo "User Name: " $user_name >> ${2}.txt
      echo "Host Name: " $host_name >> ${2}.txt
      echo "Port Number: " $port_number >> ${2}.txt
      echo "hivedb :" $hive_db_name >> ${2}.txt
      #echo "hivetb :" $hive_tb_name >> ${2}.txt
IFS='| ' read -r -a array <<< "$db_name"
 for element in "${array[@]}"
 do
               #echo "$element" >> ${2}.txt
               begin1=$(date +"%s")
               db_name="$( echo "$element" | awk -F. '{print $1}')"
               table_name="$( echo "$element" | awk -F. '{print $2}')"
               check_colm="$( echo "$element" | awk -F. '{print $3}')"
               if [ -z "$hive_db_name" ]; then
                    hive_db_name=$db_name
               fi 
               hive_tb_name=$table_name
               echo "Importing table $db_name.$table_name"
               echo "Database Name: " $db_name >> ${2}.txt
               #echo "Schema Name: " $schema_name
               echo "Table Name : " $table_name >> ${2}.txt
               echo "Check Column : " $check_colm >> ${2}.txt
              
               hive --hiveconf myvar=$hive_db_name.$hive_tb_name --hiveconf ccol=$check_colm  -e 'select max(${hiveconf:ccol}) from ${hiveconf:myvar}' 2>> ${2}.log 1>maxvalue.txt
               sed -i.bak "/WARN/d" maxvalue.txt >> ${2}.log
               var=$(<maxvalue.txt)
               echo "var:$var"
               var2=-1
                nohup hive --hiveconf myvar=$hive_db_name.$hive_tb_name -e 'desc ${hiveconf:myvar};' 2>> ${2}.log 1>schema.txt
                sed -i.bak "/WARN/d" schema.txt >> ${2}.log
                nohup hive --hiveconf myvar=$hive_db_name.$hive_tb_name -e 'desc formatted ${hiveconf:myvar};' 2>> ${2}.log 1>schema_b.txt
                sed -i.bak "/WARN/d" schema_b.txt >> ${2}.log    
                awk NF schema.txt | sed 's/\t/,/g' | awk -F, '{print $1}' > schema_t.txt
                sed -i.bak "/WARN/d" schema_t.txt >> ${2}.log 
                found=$(fgrep -c "# Partition Information" schema_t.txt)
                bucketnum=$(grep -n "Num Buckets" schema_b.txt| awk -F  ":" '{print $3}' | awk -F "\t" '{print $2}')
                flag=0
                if [ $found -eq 1 ]; then
                 flag=0
                 sed -i.bak "/col_name/d" schema_t.txt
                 sed -e '/# Partition Information/,$d' schema_t.txt > schema_1.txt
                 sed -e '1,/# Partition Information/d' schema_t.txt > schema_2.txt
                 sed -i.bak '$!s/$/,/' schema_1.txt
                 sed -i.bak '$!s/$/,/' schema_2.txt
                 echo "RDBMS Count For $db_name.$table_name is :" >> ${2}.txt
                 sqoop eval --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --query "SELECT count(*) AS MYSQL_COUNT FROM $table_name where id > $var " 2>> ${2}.log 1>> ${2}.txt
                 sqoop import --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --table $table_name --hive-import --hive-table ${hive_db_name}.${hive_tb_name}_temp -m1  --incremental append --check-column $check_colm  --last-value $var &>> ${2}.log
                 if [ $? != 0 ]; then
                   flag=1
                 fi
                 value=$(<schema_1.txt)
                 value1=$(<schema_2.txt)
                 echo  "SET hive.exec.dynamic.partition = true; set hive.exec.dynamic.partition.mode = nonstrict; 
                       insert into table $hive_db_name.$hive_tb_name partition($value1) select $value from $hive_db_name.${hive_tb_name}_temp" > script.hql
                 hive -f script.hql 2>> ${2}.log
                 if [ $? != 0 ] || [ $flag != 0 ]; then
                   echo "import for $db_name.$table_name is failed" >> ${2}.txt
                   touch failure/${2}_${db_name}_${table_name}
                   echo "appid=${2}" >> failure/${2}_${db_name}_${table_name}
                   filecontent=$(<job_1.properties)
                   echo "$filecontent" >> failure/${2}_${db_name}_${table_name}
                   sed -i.bak "/mysql_db_tb_check_col/d" failure/${2}_${db_name}_${table_name}
                   echo "mysql_db_tb_check_col=$element" >> failure/${2}_${db_name}_${table_name}
                 else
                   echo "import for $db_name.$table_name is success" >> ${2}.txt
                   echo "Hive Count For $db_name.$table_name is :" >> ${2}.txt
                   #echo "var=$var" >> ${2}.txt
                   nohup hive --hiveconf myvar=$hive_db_name.$hive_tb_name --hiveconf myvar2=$var -e 'select count(*) from ${hiveconf:myvar} where id > ${hiveconf:myvar2};' 2>> ${2}.log 1>> ${2}.txt
                   #hive --hiveconf myvar=$ht --hiveconf myvar2=$var -e 'select count(*) from ${hiveconf:myvar} where id > ${hiveconf:myvar2};' 2>> ${2}.log 1>> ${2}.txt
                 fi
                 ht=$hive_db_name.${hive_tb_name}_temp
                 nohup hive --hiveconf myvar=$ht -e 'drop table ${hiveconf:myvar};' 2>> ${2}.log
                      
               elif [ $bucketnum -ne $var2 ]; then
                  flag=0
                  echo "RDBMS Count For $db_name.$table_name is :" >> ${2}.txt
                 sqoop eval --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --query "SELECT count(*) AS MYSQL_COUNT FROM $table_name where id > $var" 2>> ${2}.log 1>> ${2}.txt
                  sqoop import --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --table $table_name --hive-import --hive-table $hive_db_name.${hive_tb_name}_temp -m1  --incremental append --check-column $check_colm  --last-value $var &>> ${2}.log
                  if [ $? != 0 ]; then
                   flag=1
                  fi  
                  echo "SET hive.enforce.bucketing = true; insert into table $hive_db_name.$hive_tb_name select * from $hive_db_name.${hive_tb_name}_temp" > script.hql
                  hive -f script.hql 2>> ${2}.log
                  if [ $? != 0 ] || [ $flag != 0 ]; then
                   echo "import for $db_name.$table_name is failed" >> ${2}.txt
                   touch failure/${2}_${db_name}_${table_name}
                   echo "appid=${2}" >> failure/${2}_${db_name}_${table_name}
                   filecontent=$(<job_1.properties)
                   echo "$filecontent" >> failure/${2}_${db_name}_${table_name}
                   sed -i.bak "/mysql_db_tb_check_col/d" failure/${2}_${db_name}_${table_name}
                   echo "mysql_db_tb_check_col=$element" >> failure/${2}_${db_name}_${table_name}
                  else
                   echo "import for $db_name.$table_name is success" >> ${2}.txt                   
                   #echo "var=$var" >> ${2}.txt
                   echo "Hive Count For $db_name.$table_name is :" >> ${2}.txt
                   nohup hive --hiveconf myvar=$hive_db_name.$hive_tb_name --hiveconf myvar2=$var -e 'select count(*) from ${hiveconf:myvar} where id > ${hiveconf:myvar2};' 2>> ${2}.log 1>> ${2}.txt 
                   #hive --hiveconf myvar=$ht --hiveconf myvar2=$var -e 'select count(*) from ${hiveconf:myvar} where id > ${hiveconf:myvar2};' 2>> ${2}.log 1>> ${2}.txt
                  fi
                   ht=$hive_db_name.${hive_tb_name}_temp
                   hive --hiveconf myvar=$ht -e 'drop table ${hiveconf:myvar};' 2>> ${2}.log
               else
                    sqoop eval --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --query "SELECT count(*) AS MYSQL_COUNT FROM $table_name where id > $var" 2>> ${2}.log 1>> ${2}.txt
                    sqoop import --connect jdbc:$data_source://$host_name:$port_number/$db_name --username $user_name --password $password --table $table_name --hive-import --hive-table $hive_db_name.${hive_tb_name} -m1  --incremental append --check-column $check_colm  --last-value $var &>> ${2}.log
                if [ $? != 0 ]; then
                   echo "import for $db_name.$table_name is failed" >> ${2}.txt
                   touch failure/${2}_${db_name}_${table_name}
                   echo "appid=${2}" >> failure/${2}_${db_name}_${table_name}
                   filecontent=$(<job_1.properties)
                   echo "$filecontent" >> failure/${2}_${db_name}_${table_name}
                   sed -i.bak "/mysql_db_tb_check_col/d" failure/${2}_${db_name}_${table_name}
                   echo "mysql_db_tb_check_col=$element" >> failure/${2}_${db_name}_${table_name}
                else
                   echo "import for $db_name.$table_name is success" >> ${2}.txt
                   #echo "var=$var" >> ${2}.txt
                   echo "Hive Count For $db_name.$table_name is :" >> ${2}.txt
                   nohup hive --hiveconf myvar=$hive_db_name.$hive_tb_name --hiveconf myvar2=$var -e 'select count(*) from ${hiveconf:myvar} where id > ${hiveconf:myvar2};' 2>> ${2}.log 1>> ${2}.txt 
                   #hive --hiveconf myvar=$ht --hiveconf myvar2=$var -e 'select count(*) from ${hiveconf:myvar} where id > ${hiveconf:myvar2};' 2>> ${2}.log 1>> ${2}.txt
                fi
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
 # source ./appid.bash
 appid $x1
 echo "Application ID: $aid"
 echo "In Progress..."
 echo "Application Started" >> ${aid}.log
 echo "Application ID: $aid" >> ${aid}.log
 echo "Error: there are $(ls failure | wc -l) files in failure folder please move file from failure dir to tobeprocessed dir and again run the script" >> ${aid}.log
elif [ $(ls $x1/tobeprocessed | wc -l) -ge 1 ]; then
  for FILE in $x1/tobeprocessed/*; do
  aid=$(grep -i 'appid' $FILE  | cut -f2 -d'=')
  echo "Application ID: $aid"
  echo "In Progress..."
  echo "Application Started" >> ${aid}.log
  echo "Application ID: $aid" >> ${aid}.log
  sqoop_incremental_import "$FILE" "$aid" 2>> ${aid}.log
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
 sqoop_incremental_import "$FILE" "$aid" 2>> ${aid}.log
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
