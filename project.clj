(defproject Monitor "0.0.9"
  :description "FIXME: write"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
		 [jfree/jfreechart "1.0.13"]
		 [jfree/jcommon "1.0.16"]
		 [my/jchart2d "3.1.0"]
		 [clojure-csv "1.2.4"]
                 [org.apache.httpcomponents/httpclient "4.0.3"]
                 [commons-codec "1.4"]
                 [commons-io "1.4"]
;                 [org.jboss.netty/netty "3.2.4.Final"]
                 [com.google.guava/guava "r09"]
                 [com.google.protobuf/protobuf-java "2.4.0a"]
                 [asm/asm-commons "3.3.1"]]
  :min-lein-version "1.5.2" 
  :dev-dependencies [[swank-clojure "1.2.1"]]
  :javac-options {:debug "true"}
  :java-source-path "src"
  :aot [#"^monitor.*" agent.Transformer]
  :main monitor.main
  :manifest {"Class-Path" "."
	     "Premain-Class" "agent.Main"}
  :test-path "src"
  :warn-on-reflection true
					;:jvm-opts [ "-Dstayalive=true" "-Dcom.sun.management.jmxremote.port=3333" "-Dcom.sun.management.jmxremote.ssl=false" "-Dcom.sun.management.jmxremote.authenticate=false"]
  :jvm-opts [ "-Dstayalive=true"]
  :repositories {"jboss" "http://repository.jboss.com/maven2/"
		 "oracle" "http://download.oracle.com/maven"
		 "mavenlocalrepo" "file://mavenlocalrepo"
                 "ow2" "http://maven.ow2.org/maven2/"}
)