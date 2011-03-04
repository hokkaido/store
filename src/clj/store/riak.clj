(ns store.riak
  (:require [clj-http.client :as client]
            [ring.util.codec :as ring]
            [clojure.string :as str]
            [clj-json.core :as json]
            [clj-time.coerce :as time-coerce])
  (:use store.api
        plumbing.core
        plumbing.streams)
  (:import java.text.SimpleDateFormat
           org.joda.time.DateTime))

(defn decode-json-bodys [json-bodys]
  (->> json-bodys
       (map  (comp #(get % "keys") json/parse-string))
       (remove empty?)
       flat-iter
       iterator-seq
       (map ring/url-decode)))

(def rfc2822-formatter
  (SimpleDateFormat. "EEE, dd MMM yyyy HH:mm:ss Z"))

(defn ^DateTime parse-rfc2822 [^String d]
  (time-coerce/from-date
   (.parse rfc2822-formatter d)))

(defn riak-bucket [& {:keys [server,name,port,prefix,bucket-config]
                      :or {server "http://127.0.0.1"
                           prefix "riak"
                           port 8098}}]
  ;; Bucket config
  (let [req-base [(str server ":" port) prefix (ring/url-encode name)]
        mk-path #(str/join "/" (concat req-base %&))
        mk-json (fn [o] {:body (.getBytes (json/generate-string o) "UTF8")
                         :content-type "application/json" :accepts :json})]
    ;; IBucket Implementatin
    (reify
      store.api.IReadBucket
      (bucket-get
       [this k]
       (-log> k str ring/url-encode mk-path client/get
              :body (json/parse-string)))
      (bucket-seq
       [this]
       (default-bucket-seq this))
      (bucket-keys
       [this]
       (-> (mk-path)
           (client/get {:query-params {"keys" "stream"}
                        :chunked? true})
           :body
           decode-json-bodys))
      (bucket-exists?
       [this k]
       (default-bucket-exists? this k))
      (bucket-modified
       [this k]
       (if-let [^String d (-> (str k)
                              ring/url-encode
                              mk-path
                              client/get
                              :headers
                              (get "last-modified"))]
         (parse-rfc2822 d)
         nil))

      store.api.IWriteBucket
      (bucket-put
       [this k v]
       (-> k str ring/url-encode mk-path (client/post (mk-json v))))  
      (bucket-delete
       [this k]
       (-> k ring/url-encode mk-path client/delete))	  
      (bucket-update
       [this k f]
       (default-bucket-update this k f))
      (bucket-sync
       [this]
       nil)
      (bucket-close
       [this]
       nil))))

(defn riak-buckets [{:keys [riak-host, riak-port]} keyspace]
  (map-from-keys
   (fn [n] (riak-bucket :name n
                        :server riak-host
                        :port riak-port))
   keyspace))