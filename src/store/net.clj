(ns store.net
  (:use store.api
        [clojure.java.io :only [file copy]]
        [clojure.contrib.shell :only [sh]]
        [plumbing.core :only [with-timeout
                              keywordize-map]]
	[plumbing.error :only [with-ex logger]]
        [clojure.string :only [lower-case]]
        [ring.adapter.jetty :only [run-jetty]]
        [compojure.core :only [GET POST PUT routes]]
        [ring.util.codec :only [url-decode url-encode]])
  (:require [clojure.string :as str]
            [clj-json.core :as json]
            [fetcher.core :as client])
  (:import (java.net Socket InetAddress
                     ServerSocket SocketException)
           (java.io InputStreamReader BufferedReader
                    PrintWriter OutputStream InputStream)
           (java.util.concurrent Executors Future Callable TimeUnit)
           (org.apache.commons.io IOUtils)))

(defn exec-req
  [buckets p & args]
  (let [mk-response (partial with-ex (logger)
                      (fn [status s]
                        {:body (json/generate-string s)
                         :headers {"Content-Type" "application/json; charset=UTF-8"}
                         :status status}))
        bucket (buckets (p :name))
        bucket-op ((merge read-ops write-ops) (keyword (p :op)))]
    (cond
     (nil? bucket) (mk-response 500 {:error (str "Don't recognize bucket " (p :name))})
     (nil? bucket-op) (mk-response 500 {:error (str "Don't recognize op " (p :op))})
     :else (try
             (mk-response 200 (apply bucket-op bucket args))
             (catch Exception e
               (.printStackTrace e)
               (mk-response 500 {:error (str e)}))))))

(defn rest-bucket-handler [buckets]
  (let [exec-request (partial with-ex (logger) exec-req buckets)]
    [ ;; seq, keys, sync, close    
     (GET "/store/:op/:name" {p :params} (exec-request  p))
     ;; batch-get
     (POST "/store/:op/:name" {p :params b :body}
           (exec-request 
                         (keywordize-map p)
                         (json/parse-string (IOUtils/toString ^java.io.InputStream b "UTF8"))))
     ;; get, modified, exists
     (GET "/store/:op/:name/:key"  {p :params} (exec-request 
                                                             p
                                                             (url-decode (p :key))))
     ;; put, merge
     (POST "/store/:op/:name/:key" {p :params b :body}
           (exec-request 
                         (keywordize-map p)
                         (url-decode (p :key))
                         (json/parse-string (IOUtils/toString ^java.io.InputStream b "UTF8"))))]))

(defn start-rest-bucket-server [buckets & {:keys [port,join?]
                                           :or {port 4445 join? false}
                                           :as jetty-opts}]
  (run-jetty (apply routes
                    (rest-bucket-handler buckets))
             jetty-opts))

(defn rest-bucket [& {:keys [name,host,port,keywordize-map?]
                      :or {host "localhost"
                           port 4445
                           keywordize-map? false}}]
  (when (nil? name)
    (throw (RuntimeException. "Must specify rest-bucket name")))
  
  (let [base (format "http://%s:%d/store/" host port)
        exec-request (partial with-ex (logger)
                       (fn [[op & as] & [body-arg]]
                         (let [url (str base
                                        (str/join "/"
                                                  (concat [op name] as)))
                               resp (if-not body-arg
                                      (client/fetch :get url)				    
                                      (client/fetch :post 
						    {:url url
						     :body (.getBytes
							    (json/generate-string body-arg)
                                                              "UTF8")}))]
                           (if (= (:status resp)
                                  200) 
                             (-> resp :body json/parse-string)
                             (throw (RuntimeException.
                                     (format "Rest bucket server error: %s"
                                             (:body resp))))))))
        my-url-encode (partial with-ex (logger)
                        (fn [k]
                          (-> k url-encode (.replaceAll "\\." "%2e"))))]
    (reify 
      store.api.IReadBucket
      (bucket-get [this k] (exec-request ["get" (my-url-encode k)]))
      (bucket-seq [this] (exec-request ["seq"]))
      (bucket-exists? [this k] (exec-request [(my-url-encode "exists?") (my-url-encode k)]))
      (bucket-keys [this] (exec-request ["keys"]))
      (bucket-batch-get [this ks] (exec-request ["batch-get"] ks))
      (bucket-modified [this k] (exec-request ["modified" (my-url-encode k)]))

      store.api.IWriteBucket
      (bucket-put [this k v] (exec-request ["put" (my-url-encode k)] v))
      (bucket-delete [this k] (exec-request ["delete" (my-url-encode k)]))
      (bucket-update [this k f] (default-bucket-update this k f))
      (bucket-merge [this k v] (exec-request ["merge" (my-url-encode k)] v))
      (bucket-close [this] (exec-request ["close"]))
      (bucket-sync [this] (exec-request ["sync"])))))