(defproject store "0.2.4-SNAPSHOT"
  :description "Distributed Datastorage Abstraction"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [woven/clj-json "0.3.2-SNAPSHOT"]
                 [net.java.dev.jets3t/jets3t "0.7.4"]
                 [clj-serializer  "0.1.1"]
                 [clj-sys/plumbing "0.1.5-SNAPSHOT"]
		 [fetcher "0.0.5-SNAPSHOT"]
                 [ring/ring-core "0.3.1"]
                 [com.sleepycat/je "4.1.7"]
                 [commons-io "2.0.1"]
                 [compojure "0.6.1"]
                 [ring/ring-core "0.3.7"]
                 [ring/ring-jetty-adapter "0.3.7"]
                 [clj-time "0.3.0-SNAPSHOT"]]
  :dev-dependencies [[swank-clojure "1.3.0-SNAPSHOT"]]
  :jvm-opts ["-Xmx600m"]
  :repositories {"oracle" "http://download.oracle.com/maven/"})
                 
                 
