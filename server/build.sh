#!/bin/bash
rm -rf classes
mkdir classes
javac -g -sourcepath src -d classes src/se/sj/monitor/*.java 
java -classpath src:classes:lib/* clojure.main -e "(compile 'se.sj.monitor.main)"
cd classes
for e in `ls ../lib`; do jar xf ../lib/$e; done; 
rm -rf META-INF
cd ..
rm -rf release
mkdir release
java -cp lib/*:src clojure.main src/jarAll.clj src/manifest classes src release.jar
#jar cfm release/monitor.jar src/manifest
#directories=""
#for e in `find classes -type d`; do 
# if [ ! -z ${e:8} ]
# then
# directories=`echo $directories "-C classes" ${e:8}`;
# fi
#done 

#echo "jar uf release/monitor.jar " $directories;
#jar uf release/monitor.jar $directories; 

#for e in `find src -type d`; do
#	jar uf release/monitor.jar -C src ${e:4}/*.java ${e:4}/*.clj;
#	done;

