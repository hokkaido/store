(ns store.net
  (:use store.api
        [clojure.java.io :only [file copy]]
        [clojure.contrib.shell :only [sh]]
        [plumbing.serialize :only [reader writer read-str-msg write-str-msg]]
        [plumbing.server :only [client server start]]
        [plumbing.core :only [print-all with-timeout with-log keywordize-map]]
        [clojure.string :only [lower-case]]
        [ring.adapter.jetty :only [run-jetty]]
        [compojure.core :only [GET PUT routes]]
        [ring.util.codec :only [url-decode url-encode]])
  (:require [clj-json.core :as json]
            [clj-http.client :as client])
  (:import (java.net Socket InetAddress
                     ServerSocket SocketException)
           (java.io InputStreamReader BufferedReader
                    PrintWriter OutputStream InputStream)
           (java.util.concurrent Executors Future Callable TimeUnit)
           (org.apache.commons.io IOUtils)))

(def op-map
  {:get bucket-get
   :exists bucket-exists?
   :keys bucket-keys
   :seq bucket-seq
   :modified bucket-modified
   :merge bucket-merge
   :put bucket-put
   :delete bucket-delete
   :sync bucket-sync
   :close bucket-close})

;; (defn- old-bucket-server [buckets]
;;   (with-log :error
;;     (fn [[op bname & args]]
;;       (let [op-key (-> op lower-case keyword)
;;             b (buckets (-> bname keyword))
;;             bop (op-map op-key)]
;;         [(apply bop b args)]))))

;; (defn old-start-net-bucket-server [buckets port]
;;   (start
;;    (with-log :error
;;      (partial server (old-bucket-server
;; 		      buckets)
;;               (comp read-str-msg reader)
;;               (fn [s msg]
;;                 (write-str-msg (writer s) msg))))
;;    :port port))

(defn bucket-server [buckets]
  (with-log :error
    (fn [^Socket s]
      (let [^InputStream in (.getInputStream s)
            ^OutputStream out (.getOutputStream s)
            r (-> in
                  (InputStreamReader.)
                  (BufferedReader.))
            w (PrintWriter. out)]
        (try 
          (let [[op bname & args] (read-str-msg r)
                op-key (-> op lower-case keyword)
                b (buckets (-> bname keyword))
                bop (op-map op-key)]
            (write-str-msg w [(apply bop b args)])
            (.flush w))
          (catch Exception e
            (.printStackTrace e))
          (finally
           (.close out)
           (.close in)
           (.close s)))))))

(defn start-net-bucket-server [buckets port]
  (start (bucket-server buckets)
         port
         "127.0.0.1"))

(defn rest-bucket-handler [buckets]
  [(GET "/buckets/:name/:key" {p :params}
	(println p)
	(let [b (buckets (url-decode (p :name)))
	      k (url-decode (p :key))]	    
	  {:body (json/generate-string (bucket-get b k))
	   :content-type "application/json"
	   :status 200}))
   (PUT "/buckets/:name/:key" {p :params body :body}
	(let [k (url-decode (p :key))
	      v (json/parse-string (IOUtils/toString body "UTF8"))
	      b (buckets (url-decode (p :name)))]
	  (try (bucket-put b k v)
	       {:body  (json/generate-string {"status" "ok"})
		:content-type "application/json"
		:status 200}
	       (catch Exception e
		 {:body (json/generate-string {"error"
					       ((print-all :ex :stack :args :fn)
						e bucket-put [k v])})
		  :status 500
		  :content-type "application/json"}))))])

(defn start-rest-bucket-server [buckets port]
  (run-jetty (apply routes (rest-bucket-handler buckets)) {:port port :join false}))

(defn rest-bucket [name host port & [keywordize-map?]]
  (let [base (format "http://%s:%d/buckets/%s/" host port name)
	keywordize (if keywordize-map? keywordize-map identity)]
    (reify
     store.api.IReadBucket
     (bucket-get [this k]
	  (let [resp (client/get (str base (url-encode k)))]
	    (cond
	      (= (:status resp) 200)
	        (-> resp :body json/parse-string keywordize)
	      :else
	        (throw (RuntimeException. (str "server error: " (-> resp :body (get "error"))))))))

     store.api.IWriteBucket
     (bucket-put [this k v]
       (let [resp  (client/put (str base (url-encode k)) {:body (.getBytes (json/generate-string v) "UTF8")})]
	 (cond
	   (= (:status resp) 200) (:body resp)
	   :else (throw (RuntimeException. (str "server error: " (-> resp :body (get "error")))))))))))


(defn net-bucket-client [host port]
  (partial client host port
           (comp first read-str-msg reader)
           (fn [s msg]
             (write-str-msg (writer s) msg))))

(defn mk-client-exec [c num-threads future-policy]
  (let [pool (Executors/newFixedThreadPool num-threads)]
    [(with-log :error
	(fn [args]
	  (let [^Callable work #(c args)
		f (.submit pool work)]
	    (future-policy f))))
     pool]))

(defn net-bucket
  "Provides bucket impl for a network interface to a store."
  [& {:keys [^String name
             ^String host
             port
             timeout
	     wait-on-write
	     writer-threads
	     reader-threads]
      :or {timeout 10
	   writer-threads 10
	   reader-threads 10
	   wait-on-write true}}]
  ;; Client will later use a pool
  (let [c (net-bucket-client host port)
	reader-exec c
	writer-exec c
	;; [reader-exec _] (mk-client-exec c reader-threads
	;; 			 (fn [^Future f]
	;; 			   (.get f (int timeout) (TimeUnit/SECONDS))))
	;; [writer-exec _] (mk-client-exec c writer-threads
	;; 			 (fn [^Future f]
	;; 			   (when wait-on-write
	;; 			     (.get f (int timeout) (TimeUnit/SECONDS)))))
	]
    (reify
      IReadBucket
      (bucket-get [this k] (reader-exec ["GET" name k]))                   
      (bucket-keys [this] (reader-exec ["KEYS" name]))                   
      (bucket-seq [this] (reader-exec ["SEQ" name]))                  
      (bucket-exists? [this k] (reader-exec ["EXISTS" name k]))
                       
      IWriteBucket
      (bucket-put [this k v] (writer-exec ["PUT" name k v]))
      (bucket-delete [this k] (writer-exec ["DELETE" name k]))		     
      (bucket-update [this k f] (writer-exec ["UPDATE" name k]))		     
      (bucket-sync [this] (writer-exec ["SYNC" name]))		   
      (bucket-close [this] (writer-exec ["CLOSE" name]))	   
      (bucket-merge [this k v] (writer-exec ["MERGE" name k v])))))		   