(ns store.message
  (:use [clojure.string :only [lower-case]])
  (:import (org.apache.commons.io IOUtils)
           (java.nio ByteBuffer)
           (java.io InputStream OutputStream)
           (java.util Arrays)))

(defn cr? [x]
  (= 13 x))

(defn nl? [x]
  (= 10 x))

(defn ubyte [x]
  (if (> x 127)
    (byte (- x 256))
    (byte x)))

(defn as-int [^Byte b]
  (bit-and b 0xFF))

(defn ^String read-ln [^InputStream is]
  (let [bb (ByteBuffer/allocate 32)]
    (loop [last-val 0]
      (let [cur-val (as-int (.read is))]
        (if (and (cr? last-val)
                 (nl? cur-val))
          (String. (Arrays/copyOf (.array bb)
                                  (- (.position bb) 1)))
          (do
            (.put bb (ubyte cur-val))
            (recur cur-val)))))))

(defn arg-count? [^String x]
  (.startsWith x "*"))

(defn arg-len? [^String x]
  (.startsWith x "$"))

(defn read-arg-count [^InputStream is]
  (let [l (read-ln is)]
    (if (arg-count? l)
      (Integer/parseInt (.substring l 1))
      (throw (Exception. "Error parsing arg count.")))))

(defn read-arg-len [^InputStream is]
  (let [l (read-ln is)]
    (if (arg-len? l)
      (Integer/parseInt (.substring l 1))
      (throw (Exception. "Error parsing arg length.")))))

(defn read-arg [^InputStream is]
  (let [arg-len (read-arg-len is)
        buf (byte-array arg-len)]
    (.read is buf)
    (.skip is 2)
    buf))

(defn read-msg [^InputStream is]
  (let [arg-count (read-arg-count is)
        args (map #(String. %)
                  (repeatedly arg-count #(read-arg is)))]
    args))


(defn write-arg-count [^OutputStream os c]
  (.write os (.getBytes
              (format "*%d\r\n" c))))

(defn write-arg [^OutputStream os arg]
  (let [arg-str (str arg)
        arg-len (count arg-str)]
    (.write os (.getBytes
                (format "$%d\r\n%s\r\n"
                        arg-len arg-str)))))

(defn write-msg [^OutputStream os args]
  (let [num-args (count args)]
    (write-arg-count os num-args)
    (doseq [arg args]
      (write-arg os arg))))