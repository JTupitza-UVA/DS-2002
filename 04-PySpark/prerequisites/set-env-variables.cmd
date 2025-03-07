echo "Setting SPARK_HOME environmental variable"
setx SPARK_HOME "C:\spark-3.5.4-bin-hadoop3"

echo "Setting HADOOP_HOME environmental variable"
setx HADOOP_HOME "C:\hadoop-3.3.5"

echo "Setting JAVA_HOME environmental variable"
setx JAVA_HOME "C:\Java\jdk-21"

echo "Appending New Environmental Variable to PATH"
for /f "tokens=2*" %A in ('reg query "HKCU\Environment" /v Path') do set "oldPath=%B"

setx Path %oldPath%;%SPARK_HOME%\bin;%HADOOP_HOME%\bin;%JAVA_HOME%\bin"