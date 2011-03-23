(ns store.net
  (:use store.api
        [clojure.java.io :only [file copy]]
        [clojure.contrib.shell :only [sh]]
        [plumbing.serialize :only [reader writer read-str-msg write-str-msg]]
        [plumbing.server :only [client server start]]
        [plumbing.core :only [print-all with-timeout with-log keywordize-map]]
        [clojure.string :only [lower-case]]
        [ring.adapter.jetty :only [run-jetty]]
        [compojure.core :only [GET POST PUT routes]]
	[ring.util.codec :only [url-decode url-encode]])
  (:require 	[clojure.string :as str]
            [clj-json.core :as json]
            [clj-http.client :as client])
  (:import (java.net Socket InetAddress
                     ServerSocket SocketException)
           (java.io InputStreamReader BufferedReader
                    PrintWriter OutputStream InputStream)
           (java.util.concurrent Executors Future Callable TimeUnit)
           (org.apache.commons.io IOUtils)))

(defn rest-bucket-handler [buckets]
  (let [mk-response (with-log :error
		      (fn [status s]
		       {:body (json/generate-string s)
			:headers {"Content-Type" "application/json; charset=UTF-8"}
			:status status}))
	exec-request (with-log :error
		       (fn [p & args]
			(let [bucket (buckets (p :name))
			      bucket-op ((merge read-ops write-ops) (keyword (p :op)))]
			  (cond
			   (nil? bucket) (mk-response 500 {:error (str "Don't recognize bucket " (p :name))})
			   (nil? bucket-op) (mk-response 500 {:error (str "Don't recognize op " (p :op))})
			   :else (try
				   (mk-response 200 (apply bucket-op bucket args))
				   (catch Exception e
				     (.printStackTrace e)
				     (mk-response 500 {:error (str e)})))))))]
    [ ;; seq, keys, sync, close    
     (GET "/store/:op/:name" {p :params} (exec-request p))
     ;; batch-get
     (POST "/store/:op/:name" {p :params b :body}
	   (exec-request (keywordize-map p)
			 (json/parse-string (IOUtils/toString ^java.io.InputStream b "UTF8"))))
     ;; get, modified, exists
     (GET "/store/:op/:name/:key"  {p :params} (exec-request p (url-decode (p :key))))
     ;; put, merge
     (POST "/store/:op/:name/:key" {p :params b :body}
	   (exec-request (keywordize-map p)
			 (url-decode (p :key))
			 (json/parse-string (IOUtils/toString ^java.io.InputStream b "UTF8"))))]))

(defn start-rest-bucket-server [buckets & {:keys [port,join?] :or {port 4445 join? false} :as jetty-opts}]
  (run-jetty (apply routes (rest-bucket-handler buckets)) jetty-opts))

(defn rest-bucket [& {:keys [name,host,port,keywordize-map?]
		      :or {host "localhost"
			   port 4445
			   keywordize-map? false}}]
  (when (nil? name) (throw (RuntimeException. "Must specify rest-bucket name")))
  (let [base (format "http://%s:%d/store/" host port name)
	exec-request (with-log :error
		       (fn [[op & as] & [body-arg]]
			(let [uri (str base (str/join "/" (concat [op name] as)))
			      resp (if-not body-arg (client/get uri)				    
					   (client/post uri
							{:body (.getBytes (json/generate-string body-arg) "UTF8")}))]
			  (if (= (:status resp) 200) 
			    (-> resp :body json/parse-string)
			    (throw (RuntimeException. (format "Rest bucket server error: %s" (:body resp))))))))
	my-url-encode (with-log :error (fn [k] (-> k url-encode (.replaceAll "\\." "%2e"))))]
   (reify 
     store.api.IReadBucket
     (bucket-get [this k] (exec-request ["get" (my-url-encode k)]))
     (bucket-seq [this] (exec-request ["seq"]))
     (bucket-exists? [this k] (exec-request ["exists?" (my-url-encode k)]))
     (bucket-keys [this] (exec-request ["keys"]))
     (bucket-batch-get [this ks] (exec-request ["batch-get"] ks))
     (bucket-modified [this k] (exec-request ["modified" (my-url-encode k)]))

     store.api.IWriteBucket
     (bucket-put [this k v] (exec-request ["put" (my-url-encode k)] v))
     (bucket-delete [this k] (exec-request ["delete" (my-url-encode k)]))
     (bucket-merge [this k v] (exec-request ["merge" (my-url-encode k)] v))
     (bucket-close [this] (exec-request ["close"]))
     (bucket-sync [this] (exec-request ["sync"])))))