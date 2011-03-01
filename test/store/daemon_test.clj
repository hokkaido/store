(ns store.daemon-test
  (:use clojure.test
        [store.api :only [hashmap-bucket bucket-put]]
        [store.net :only [client-socket req]]
        [clojure.contrib.server-socket :only [close-server]])
  (:use store.daemon :reload)
  (:import (java.net InetAddress)
           (java.io ByteArrayOutputStream)
           (org.apache.commons.io IOUtils)))

(deftest handler-test
  (let [baos (ByteArrayOutputStream.)]
    ((handler {:b1 (doto (hashmap-bucket)
                     (bucket-put "k1" "val1"))
               :b2 (hashmap-bucket)})
     (IOUtils/toInputStream
      "*3\r\n$3\r\nGET\r\n$2\r\nb1\r\n$2\r\nk1\r\n")
     baos)
    (is (= "*1\r\n$4\r\nval1\r\n"
           (String. (.toByteArray baos))))))

(def client (partial client-socket "127.0.0.1" 4444))

(deftest ^{:system true} server-client-test
  (let [s (start
           (handler {:b1 (hashmap-bucket)
                     :b2 (hashmap-bucket)})
           :port 4444)]
    
    (is (= ""
           (client (req ["PUT" "b1" "key1" "val1"]))))
    (is (= "val1"
           (client (req ["GET" "b1" "key1"]))))
    (is (= ""
           (client (req ["PUT" "b1" "key2" "val2"]))))
    (is (= #{"key1" "key2"}
           (-> (client (req ["KEYS" "b1"]))
               read-string
               set)))
    (is (= "val1"
           (client (req ["DELETE" "b1" "key1"]))))
    (is (= "val2"
           (client (req ["DELETE" "b1" "key2"]))))))