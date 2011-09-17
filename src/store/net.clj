(ns store.net
  (:use [clojure.java.io :only [file copy]]
        [clojure.contrib.shell :only [sh]]
        [plumbing.core :only [keywordize-map ?>]]
	[plumbing.error :only [with-ex logger]]
        [clojure.string :only [lower-case]]
	[clojure.java.io :only [reader]]
	[compojure.core :only [GET POST routes]]
	[ring.adapter.jetty :only [run-jetty]]
	[services.core :only [start-web]]
        [ring.util.codec :only [url-decode url-encode]])
  (:require [plumbing.observer :as obs]
	    [store.core :as bucket]
	    [store.api :as store]
	    [clojure.string :as str]
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

(deftype RestStore [bucket-map dispatch context]
  store/IStore
  (exec [this form]
	;;ensure form is evalable.
	(eval form)
	(rest-call (assoc context :op "exec" :body form)))
  (buckets [this _] (rest-call (assoc context :op "buckets")))
  (bucket [this bucket-name] (rest-call (assoc context :op  "bucket" :name bucket-name)))
  (add [this bucket-name]
       (store/add-bucket bucket-map bucket-name context)
       (rest-call (assoc context :op  "add" :name bucket-name)))
  (remove [this bucket-name]
	  (bucket/delete bucket-map bucket-name)
	  (rest-call (assoc context :op  "remove" :name bucket-name)))
  clojure.lang.IFn
  (invoke [this op]
	  (dispatch this op nil))
  (invoke [this op bucket-name]
	  (dispatch this op bucket-name))
  (invoke [this op bucket-name key]
	  (dispatch this op bucket-name key))
  (invoke [this op bucket-name key val]
	  (dispatch this op bucket-name key val))
  (applyTo [this args]
	   (apply dispatch this args)))

(defmethod store/store :rest [bucket-specs & [context]]
	   (let [context (update-in (or context {}) [:observer]
				    obs/sub-observer (obs/gen-key "store"))]
	     (-> (bucket/buckets bucket-specs context)
		 store/start-flush-pools
		 (RestStore.
		  (store/observe-dispatch store/dispatch context)
		  context))))

(defn parse-body [^java.io.InputStream b]
  (json/parse-string (IOUtils/toString b "UTF8")))

(defn to-json [data]
  (if-not data (json/generate-string nil)
	(try
	  (json/generate-string data)
	  ;;hack: can't serialize, so just tell the fuckers it worked.
	  (catch Exception e (json/generate-string "SUCCESS")))))

(defn rest-response-body [op data]
  (let [op (keyword op)]
    (cond (bucket/streaming-ops op)
	  (map json/generate-string data)
	  :else
	  (to-json data))))

(defn rest-response [status op data]
  (assoc {:headers
	  {"Content-Type" "application/json; charset=UTF-8"}   
	  :status status}
    :body (rest-response-body op data)))

(defn jsonp-response [callback data]
  {:status 200
   :headers {"Content-Type" "text/javascript; charset=UTF-8"}
   :body (str callback "(" (json/generate-string data) ")")})

(defn seq-post-process [query-params data]
  (if (query-params "limit")
    (take (Integer/parseInt (query-params "limit")) data)
    data))

(defn handle-request
  [s p & args]
  (let [o (p :op)
	n (p :name)
	callback (p "callback")]
    (try
      (let [data (apply s (keyword o) n args)
	    data (if (#{"seq" "keys"} o)
		   (seq-post-process p data)
		   data)]

	(when (= o "add") (println (format "tried to add %s" data)))
	
	(if callback
	  (jsonp-response callback data)
	  (rest-response 200 o data)))
      (catch Exception e
	(log/info (format "params: %s args: %s" (pr-str p) (pr-str args)))
	(.printStackTrace e)
	(rest-response 500 nil {:error (str e)})))))

(defn rest-store-handler [s]
  (let [handle (partial with-ex (logger) handle-request s)]
    [ ;; seq, keys, sync, close, add
     (GET "/store/:op" {p :params}
	  (handle p))
     (GET "/store/:op/:name" {p :params}
	  (handle p))
     ;; batch-get, batch-put, batch-merge
     (POST "/store/:op/:name" {p :params b :body}
           (handle 
	       p (parse-body b)))
     ;; get, modified, exists
     (GET "/store/:op/:name/:key"  {p :params}
	  (handle p (url-decode (p :key))))
     ;; put, merge
     (POST "/store/:op/:name/:key" {p :params b :body}
           (handle 
	       p
	     (url-decode (p :key))
	     (parse-body b)))]))

(defn store-server [s & {:keys [port]
			 :or {port 4445}}]
  (-> s
      rest-store-handler
      (start-web {:port port :join? false})))
		  
(defmethod bucket/bucket :rest [{:keys [batch-size merge] :or {batch-size 10000} :as args}]
  (let [exec (mpartial rest-call args)]
    (->
     (reify
      store.core.IReadBucket
      (bucket/get [this k] (exec {:op "get" :as k}))
      (bucket/seq [this] (exec {:op "seq"}))
      (bucket/exists? [this k] (exec {:op "exists?" :as k}))
      (bucket/keys [this] (exec {:op "keys"}))
      (bucket/count [this] (throw (UnsupportedOperationException.)))      
      (bucket/batch-get [this ks]
			(->> ks
			     (partition-all batch-size)
			     (mapcat (fn [p] (exec {:op "batch-get" :body p})))))

      store.core.IOptimizeBucket
      (bucket/optimize [this] (exec {:op "optimize"}))

      store.core.IMergeBucket
      (bucket/merge [this k v]
		    (exec {:op "merge" :as k :body v}))
      (bucket/batch-merge [this kvs] 
		    (exec {:op "batch-merge" :body kvs}))
      
      store.core.IWriteBucket
      (bucket/put [this k v] (exec {:op "put" :as k :body v}))
      (bucket/batch-put [this kvs] (exec {:op "batch-put" :body kvs}))
      (bucket/delete [this k] (exec {:op "delete" :as k}))
      (bucket/update [this k f]
		     (throw (Exception. (format "can not call update on rest bucket %s with key: %s and update fn: %s" this k f))))
      (bucket/close [this])
      (bucket/sync [this] (exec {:op "sync"})))
     (bucket/wrapper-policy args))))