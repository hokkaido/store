(ns store.message-test
  (:use clojure.test)
  (:use store.message)
  (:import (org.apache.commons.io IOUtils)
           (java.io ByteArrayOutputStream)))

(deftest read-ln-test
  (is (= "GET"
         (read-ln (IOUtils/toInputStream "GET\r\n")))))

(deftest read-arg-count-test
  (is (= 2
         (read-arg-count (IOUtils/toInputStream
                          "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")))))

(deftest read-arg-len-test
  (is (= 3
         (read-arg-len (IOUtils/toInputStream
                        "$3\r\nGET\r\n$3\r\nkey\r\n")))))

(deftest read-arg-test
  (is (= (seq "GET")
         (seq (read-arg (IOUtils/toInputStream
                         "$5\r\n\"GET\"\r\n$3\r\nkey\r\n"))))))

(deftest read-msg-test
  (let [[cmd & args] (read-msg (IOUtils/toInputStream
                                "*2\r\n$5\r\n\"GET\"\r\n$5\r\n\"key\"\r\n"))]
    (is (= "GET" cmd))
    (is (= (list "key")
           args))))


(deftest write-arg-count-test
  (let [baos (doto (ByteArrayOutputStream.)
               (write-arg-count 20))]
    (is (= "*20\r\n"
           (String. (.toByteArray baos))))))

(deftest write-arg-test
  (let [baos (doto (ByteArrayOutputStream.)
               (write-arg (pr-str 1024)))]
    (is (= "$4\r\n1024\r\n"
           (String. (.toByteArray baos))))))

(deftest write-msg-test
  (let [baos (doto (ByteArrayOutputStream.)
               (write-msg [555 "rawr"]))]
    (is (= "*2\r\n$3\r\n555\r\n$6\r\n\"rawr\"\r\n"
           (String. (.toByteArray baos))))))