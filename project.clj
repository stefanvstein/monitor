(defproject Monitor "1.0.0-SNAPSHOT"
  :description "FIXME: write"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
		 [com.sleepycat/je "4.0.92"]
		 [jfree/jfreechart "1.0.13"]
		 [jfree/jcommon "1.0.16"]
		 [my/jchart2d "3.1.0"]]
  :min-lein-version "1.3.0"
  :dev-dependencies [[swank-clojure "1.2.1"]
		     [lein-javac "1.2.1-SNAPSHOT"]]
  :java-options {:debug "true"}
  :java-source-path [["src"]]
  :hooks [leiningen.hooks.javac]
  :aot [monitor.main]
  :main monitor.main
  :manifest {"Class-Path" "."}
  :test-path "src"
  :jvm-opts [ "-Dstayalive=true" "-mx1024M" "-Dcom.sun.management.jmxremote.port=3333" "-Dcom.sun.management.jmxremote.ssl=false" "-Dcom.sun.management.jmxremote.authenticate=false"]
  :repositories {"jboss" "http://repository.jboss.com/maven2/"
		 "oracle" "http://download.oracle.com/maven"
		 "mavenlocalrepo" "file://mavenlocalrepo"})
