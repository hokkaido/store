(defproject store "0.1.4-SNAPSHOT"
  :description "Distributed Datastorage Abstraction"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [net.java.dev.jets3t/jets3t "0.7.4"]
                 [org.clojars.mattrepl/jedis "1.3.2-SNAPSHOT"]
                 [clj-serializer  "0.1.1"]
                 [clj-sys/plumbing "0.1.3-SNAPSHOT"]
                 [ring/ring-core "0.3.1"]
                 [clomert "0.2.0"]
                 [clj-serializer "0.1.0"]
                 [clj-riak "0.1.0-SNAPSHOT"]]
  :dev-dependencies [[swank-clojure "1.3.0-SNAPSHOT"]
                     [lein-clojars "0.5.0"]
                     [robert/hooke "1.1.0"]]
  :source-path "src/clj"
  :java-source-path "src/java"
  :test-selectors {:default (fn [v] (not (:system v)))
                   :system :system
                   :redis :redis
                   :riak :riak
                   :all (fn [_] true)})