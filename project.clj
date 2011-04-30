(defproject store "0.2.3-SNAPSHOT"
  :description "Distributed Datastorage Abstraction"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [woven/clj-json "0.3.2-SNAPSHOT"]
                 [net.java.dev.jets3t/jets3t "0.7.4"]
                 [org.clojars.mattrepl/jedis "1.3.2-SNAPSHOT"]
                 [clj-serializer  "0.1.1"]
                 [clj-sys/plumbing "0.1.4-SNAPSHOT"]
		 [fetcher "0.0.5-SNAPSHOT"]
                 [ring/ring-core "0.3.1"]
                 [clomert "0.2.0"]
                 [com.sleepycat/je "4.1.7"]
                 [commons-io "2.0.1"]
                 [compojure "0.6.1"]
                 [ring/ring-core "0.3.7"]
                 [ring/ring-jetty-adapter "0.3.7"]
                 [clj-time "0.3.0-SNAPSHOT"]]
  :dev-dependencies [[swank-clojure "1.3.0-SNAPSHOT"]
                     [robert/hooke "1.1.0"]]
  :jvm-opts ["-Xmx600m"]
  :test-selectors {:default (fn [v] (not (:system v)))
                   :system :system
                   :redis :redis
                   :riak :riak
                   :in-memory :in-memory
                   :performance :performance
                   :all (fn [x] (not= x :performance))}
  :repositories {"snapshots" "http://mvn.getwoven.com/repos/woven-public-snapshots"
                 "releases" "http://mvn.getwoven.com/repos/woven-public-releases"
                 "oracle" "http://download.oracle.com/maven/"})
