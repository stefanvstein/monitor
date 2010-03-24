#!/bin/bash
rm -rf classes
mkdir classes
javac -g -sourcepath src -d classes src/se/sj/monitor/*.java 
java -classpath src:classes:lib/* clojure.main -e "(compile 'se.sj.monitor.main)"
cd classes
for e in `ls ../lib`; do jar xf ../lib/$e; done; 
rm -rf META-INF
cd ..
java -cp lib/*:src clojure.main src/jarAll.clj src/manifest classes src monitor.jar
#rm -rf classes


