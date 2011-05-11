(ns store.net
  (:use store.core
        [clojure.java.io :only [file copy]]
        [clojure.contrib.shell :only [sh]]
        [plumbing.core :only [keywordize-map]]                              
	[plumbing.error :only [with-ex logger]]
        [clojure.string :only [lower-case]]
	[clojure.java.io :only [reader]]
	[compojure.core :only [GET POST routes]]
	[ring.adapter.jetty :only [run-jetty]]
        [ring.util.codec :only [url-decode url-encode]])
  (:require [clojure.string :as str]
            [clj-json.core :as json]
	    [clojure.contrib.logging :as log]
            [fetcher.core :as client])
  (:import (java.net Socket InetAddress
                     ServerSocket SocketException)
           (java.io InputStreamReader BufferedReader
                    PrintWriter OutputStream InputStream)
           (java.util.concurrent Executors Future Callable TimeUnit)
           (org.apache.commons.io IOUtils)))

(defn rest-response-base [status]
  {:headers {"Content-Type" "application/json; charset=UTF-8"}   
   :status status})

(defn parse-body [^java.io.InputStream b]
  (json/parse-string (IOUtils/toString b "UTF8")))

(defn rest-response-body [op data]
  (case op
     ("keys" "seq") (map json/generate-string data)
     (json/generate-string data)))

(defn rest-response [status op data]
  (assoc (rest-response-base status)
     :body (rest-response-body op data)))

(defn exec-request
  [s p & args]
  (try
    (rest-response 200 (p :op)
		   (apply s (keyword (p :op)) (p :name) args))
    (catch Exception e
      (log/info (format "params: %s %s" (pr-str p) (pr-str args)))
      (.printStackTrace e)
      (rest-response 500 nil {:error (str e)}))))

(defn rest-store-handler [s]
  (let [exec-req (partial with-ex (logger) exec-request s)]
    [ ;; seq, keys, sync, close    
     (GET "/store/:op/:name" {p :params}
	  (exec-req p))
     ;; batch-get
     (POST "/store/:op/:name" {p :params b :body}
           (exec-req 
	    p (parse-body b)))
     ;; get, modified, exists
     (GET "/store/:op/:name/:key"  {p :params}
	  (exec-req p (url-decode (p :key))))
     ;; put, merge
     (POST "/store/:op/:name/:key" {p :params b :body}
           (exec-req 
	    p
	    (url-decode (p :key))
	    (parse-body b)))]))

;; url (str base (str/join "/" (concat [op name] as)))
(defn exec-client-request [op url & [body-arg]]
  (if-not body-arg
    (client/fetch :get
		  {:url url
		   :as (if (#{"keys" "seq"} op)
			 :input-stream
			 :string)})				    
    (client/fetch :post 
		  {:url url
		   :body (.getBytes
			  (json/generate-string body-arg)
			  "UTF8")})))

(defn process-client-response
  [op {:keys [status,body,keywordize?]
       :or {keywordize? true}
       :as resp}]
  (if (= status 200)                                   
    (if (#{"keys" "seq"} op)
      (-> ^java.io.InputStream body
	  java.io.InputStreamReader.
	  java.io.BufferedReader.
	  (json/parsed-seq keywordize?))
      (json/parse-string body keywordize?))
    (throw (RuntimeException.
	    (format "Rest bucket server error: %s"
		    resp)))))

(defn correct-url-encode [k]
  (-> k url-encode (.replaceAll "\\." "%2e")))

(defn request-url [base & pieces]
  (str base
    (str/join "/"
	(map correct-url-encode pieces))))

(defn rest-bucket
  [& {:keys [name,host,port,keywordize?,batch-size]
      :or {host "localhost"
	   batch-size 10000
	   port 4445
	   keywordize? true}}]
  (when (nil? name)
    (throw (RuntimeException. "Must specify rest-bucket name")))
  
  (let [base (format "http://%s:%d/store/"
		     (.replaceAll host "http://" "") port)
        exec (fn [[op & as] & [body-arg]]
	       (let [url (apply request-url base op name as)]
		 (->> (exec-client-request op url body-arg)
		      (merge {:keywordize? keywordize?})
		      (process-client-response op))))]
    (reify
     store.core.IReadBucket
     (bucket-get [this k] (exec ["get" k]))
     (bucket-seq [this] (exec ["seq"]))
     (bucket-exists? [this k] (exec ["exists?" k]))
     (bucket-keys [this] (exec ["keys"]))
     (bucket-batch-get [this ks]
       (->> ks
	    (partition-all batch-size)
	    (mapcat (partial exec ["batch-get"]))))
     (bucket-modified [this k] (exec ["modified" k]))

     store.core.IWriteBucket
     (bucket-put [this k v] (exec ["put" k] v))
     (bucket-delete [this k] (exec ["delete" k]))
     (bucket-update [this k f] (default-bucket-update this k f))
     (bucket-merge [this k v] (exec ["merge" k] v))
     (bucket-close [this])
     (bucket-sync [this] (exec ["sync"])))))