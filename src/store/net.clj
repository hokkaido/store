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


(defn correct-url-encode [k]
  (-> k url-encode (.replaceAll "\\." "%2e")))

(defn request-url [base & pieces]
  (str base
    (str/join "/"
	(map correct-url-encode pieces))))

(defn mpartial [f m1]
  (fn [m2]
    (f (merge m1 m2))))

;; url (str base (str/join "/" (concat [op name] as)))
(defn exec-client-request [get-client op url & [body-arg]]
  (if-not body-arg
    (client/fetch get-client
		  :get
		  {:url url
		   :as (if (#{"keys" "seq"} op)
			 :input-stream
			 :string)})				    
    (client/fetch get-client
                  :post 
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

(defn rest-call [{:keys [host port client-ops name
			 keywordize? op as body]
		  :or {host "localhost"
		       port 4445
		       keywordize? true}}]
  (let [base (format "http://%s:%d/store/"
		     (.replaceAll host "http://" "") port)
	get-client #(client/basic-http-client client-ops)
	url (->>
	     [op name as]
	     (filter identity)
	     (apply request-url base))]
    (->> (exec-client-request get-client op url body)
	 (merge {:keywordize? keywordize?})
	 (process-client-response op))))

(defn rest-op [op store bucket-name]
  (rest-call (assoc (.context store)
			  :op op
			  :name bucket-name)))

(def rest-bucket-ops
     {:add (partial rest-op "add")
      :remove (partial rest-op "remove")
      :bucket (partial rest-op "bucket")})

(defn parse-body [^java.io.InputStream b]
  (json/parse-string (IOUtils/toString b "UTF8")))

(defn rest-response-body [op data]
  (let [op (keyword op)]
    (cond (streaming-ops op)
	  (map json/generate-string data)
	  (or (write-ops op)
	      (rest-bucket-ops op))
	  (json/generate-string (if data "SUCCESS" nil))
	  :else
	  (json/generate-string data))))

(defn rest-response [status op data]
  (assoc {:headers
	  {"Content-Type" "application/json; charset=UTF-8"}   
	  :status status}
    :body (rest-response-body op data)))

(defn exec-request
  [s p & args]
  (let [o (p :op)
	n (p :name)]
    (try
      (rest-response 200 o (apply s (keyword o) n args))
      (catch Exception e
	(log/info (format "params: %s %s" (pr-str p) (pr-str args)))
	(.printStackTrace e)
	(rest-response 500 nil {:error (str e)})))))

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

(defn store-server [s]
  (-> s
      rest-store-handler
      ((fn [x] (apply routes x)))
      (run-jetty {:port 4445
		  :join? false})))

(defn rest-bucket
  [& {:keys [batch-size]
      :or {batch-size 10000}
      :as conf}]
  (let [exec (mpartial rest-call conf)]
    (reify
     store.core.IReadBucket
     (bucket-get [this k] (exec {:op "get" :as k}))
     (bucket-seq [this] (exec {:op "seq"}))
     (bucket-exists? [this k] (exec {:op "exists?" :as k}))
     (bucket-keys [this] (exec {:op "keys"}))
     (bucket-batch-get [this ks]
       (->> ks
	    (partition-all batch-size)
	    (mapcat (fn [p] (exec {:op "batch-get" :body p})))))
     (bucket-modified [this k] (exec {:op "modified" :as k}))

     store.core.IWriteBucket
     (bucket-put [this k v] (exec {:op "put" :as k :body v}))
     (bucket-delete [this k] (exec {:op "delete" :as k}))
     (bucket-update [this k f]
		    (throw (Exception. (format "can not call update on rest bucket %s with key: %s and update fn: %s" this k f))))
     (bucket-merge [this k v]
		   (exec {:op "merge" :as k :body v}))
     (bucket-close [this])
     (bucket-sync [this] (exec {:op "sync"})))))