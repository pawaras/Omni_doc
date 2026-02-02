#!/bin/bash
PYPI_INDEX="https://nexus.nee.com/repository/pypi-proxy-repo/simple"

sudo sh -c 'echo 155.109.64.36 EVAPzen.fpl.com >> /etc/hosts'

sudo sh -c 'echo -e "export http_proxy=http://EVAPzen.fpl.com:10262" >> /etc/environment'
sudo sh -c 'echo -e "export no_proxy=\"169.254.169.254\"" >> /etc/environment'
source /etc/environment
curl http://repo.us-east-1.amazonaws.com/2018.03/main/mirror.list  -v

aws s3 cp s3://2mnv-lo174-configurations-logs-s3/package/css_data_alr_package.tar css_data_alr_package.tar
tar -xf css_data_alr_package.tar -C /home/hadoop/


# install extra linux packages (psycopg2-binary doesn't need postgresql-devel) 
#yes | sudo yum install postgresql-devel


# install extra python packages (we load all packages in the zip file)
sudo yum install -y python3-devel
sudo yum install -y python3-devel.x86_64 -y
sudo yum install gcc-c++ python-devel.x86_64 cyrus-sasl-devel.x86_64 -y
sudo python3 -m pip  install -U pip setuptools -i ${PYPI_INDEX}
sudo python3 -m pip   install cffi -i ${PYPI_INDEX}
sudo python3 -m pip   install setuptools-rust -i ${PYPI_INDEX}
sudo python3 -m pip   install  statsmodels -i ${PYPI_INDEX}
sudo python3 -m pip   install SQLAlchemy==1.4.46 -i ${PYPI_INDEX}
sudo python3 -m pip   install psycopg2-binary boto3==1.26.101 pandas smart_open[all] fuzzywuzzy python-Levenshtein sasl thrift_sasl pyhive -i ${PYPI_INDEX}
sudo yum install cyrus-sasl-md5 cyrus-sasl-plain cyrus-sasl-gssapi cyrus-sasl-devel -y
sudo python3 -m pip   install fsspec -i ${PYPI_INDEX}
sudo python3 -m pip   install pyarrow==2 -i ${PYPI_INDEX}
sudo python3 -m pip   install awswrangler==2.9.0 -i ${PYPI_INDEX}
sudo python3 -m pip   install pandasql -i ${PYPI_INDEX}
sudo python3 -m pip  install s3fs -i ${PYPI_INDEX}
sudo python3 -m pip   install pysftp -i ${PYPI_INDEX}





# install extra jars
POSTGRES_JAR="postgresql-42.2.12.jar"
SQLSERVER_JAR="mssql-jdbc-8.2.2.jre8.jar"
ORACLE_JAR="ojdbc8.jar"
COBOL_PARSER="cobol-parser-0.2.5.jar"
SCODEC_BITS="scodec-bits_2.11-1.1.4.jar"
SCODEC_CORE="scodec-core_2.11-1.10.3.jar"
SPARK_COBOL="spark-cobol-0.2.5.jar"
SAP_JAR="ngdbc-2.10.15.jar"
REDSHIFT_JAR="RedshiftJDBC42-no-awssdk-1.2.55.1083.jar"


aws s3 cp s3://2mnv-lo174-configurations-logs-s3/jars/${POSTGRES_JAR} ${POSTGRES_JAR}
aws s3 cp s3://2mnv-lo174-configurations-logs-s3/jars/${SQLSERVER_JAR} ${SQLSERVER_JAR}
aws s3 cp s3://2mnv-lo174-configurations-logs-s3/jars/${ORACLE_JAR} ${ORACLE_JAR}
aws s3 cp s3://2mnv-lo174-configurations-logs-s3/jars/${SAP_JAR} ${SAP_JAR}
aws s3 cp s3://2mnv-lo174-configurations-logs-s3/jars/${REDSHIFT_JAR} ${REDSHIFT_JAR}

aws s3 cp s3://2mnv-lo174-configurations-logs-s3/jars/${COBOL_PARSER} ${COBOL_PARSER}
aws s3 cp s3://2mnv-lo174-configurations-logs-s3/jars/${SCODEC_BITS} ${SCODEC_BITS}
aws s3 cp s3://2mnv-lo174-configurations-logs-s3/jars/${SCODEC_CORE} ${SCODEC_CORE}
aws s3 cp s3://2mnv-lo174-configurations-logs-s3/jars/${SPARK_COBOL} ${SPARK_COBOL}



mkdir -p /home/hadoop/jars/
mv ${POSTGRES_JAR} /home/hadoop/jars/
mv ${SQLSERVER_JAR} /home/hadoop/jars/
mv ${ORACLE_JAR} /home/hadoop/jars/
mv ${SAP_JAR} /home/hadoop/jars/
mv ${REDSHIFT_JAR} /home/hadoop/jars/


mv ${COBOL_PARSER} /home/hadoop/jars/
mv ${SCODEC_BITS} /home/hadoop/jars/
mv ${SCODEC_CORE} /home/hadoop/jars/
mv ${SPARK_COBOL} /home/hadoop/jars/


sudo chown -R hadoop:hadoop /home/hadoop/jars/




sudo yum install -y gcc-c++ python3-devel unixODBC-devel
sudo ln -s /usr/libexec/gcc/x86_64-redhat-linux/7/cc1plus /usr/bin/
#sudo python3 -m pip --proxy http://EVAPzen.fpl.com:10262  install pyodbc
sudo python3 -m pip  install pyodbc -i ${PYPI_INDEX}


aws s3 cp s3://2mnv-lo174-configurations-logs-s3/package/spark_env_script.sh /home/hadoop
chmod 755 /home/hadoop/spark_env_script.sh


aws s3 cp s3://2mnv-lo174-configurations-logs-s3/package/sftp.sh /home/hadoop
chmod 755 /home/hadoop/sftp.sh

aws s3 cp s3://2mnv-lo174-configurations-logs-s3/package/ttt /home/hadoop/.ssh/id_rsa
aws s3 cp s3://2mnv-lo174-configurations-logs-s3/package/t_hosts /home/hadoop/.ssh/known_hosts
chmod 600 /home/hadoop/.ssh/*








