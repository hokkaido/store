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

(defn decode-chunked-objs [os]
  (->> os
       (map #(get % "keys"))
       (remove empty?)
       flat-iter
       iterator-seq
       (map ring/url-decode)))

(def rfc2822-formatter
  (SimpleDateFormat. "EEE, dd MMM yyyy HH:mm:ss Z"))

(defn ^DateTime parse-rfc2822 [^String d]
  (time-coerce/from-date
   (.parse rfc2822-formatter d)))

(defn last-modified [resp]
  (when-let [d (-> resp
                 :headers
                 (get "last-modified"))]
    (silent parse-rfc2822 d)))

(defn riak-bucket [& {:keys [server,name,port,prefix,bucket-config]
                      :or {server "http://127.0.0.1"
                           prefix "riak"
                           port 8098}}]
  ;; Bucket config
  (let [req-base [(str server ":" port) prefix (ring/url-encode name)]
        mk-path #(str/join "/" (concat req-base %&))
        mk-json (fn [o] {:body (.getBytes (json/generate-string o) "UTF8")
                         :content-type "application/json" :accepts :json})
	exec-req (with-log :error		   
		   (fn [args req-opts]
		     (client/request
		        (assoc req-opts
			  :url (apply mk-path (map str args))))))
	read-resp (with-log :error (comp json/parse-string :body))]
    ;; IBucket Implementatin
    (reify
      store.api.IReadBucket
      (bucket-get
       [this k]
       (when-let [resp (exec-req [k] {:method :get})]
	 (when-let [v (read-resp resp)]
	  (if (not (instance? clojure.lang.IObj v))
	    v	   
	    (with-meta v
	      {:last-modified (last-modified resp)})))))
      (bucket-seq
       [this]
       (default-bucket-seq this))
      (bucket-keys
       [this]
       (when-let [resp (exec-req []
				 {:method :get
				  :query-params {"keys" "stream"}})]
	 (decode-chunked-objs
	     (if (string? (:body resp))
	       (->> resp :body (java.io.StringReader.) json/parsed-seq)
	       (->> resp :body (map json/parse-string))))))
      (bucket-exists?
       [this k]
       (default-bucket-exists? this k))
      (bucket-modified
       [this k]
       (when-let [resp (exec-req [k] {:method :head})]
	 (last-modified resp)))
      store.api.IWriteBucket
      (bucket-put
       [this k v]
       (when-let [resp (exec-req [k] (assoc (mk-json v)
				       :method :post))]
	 resp))  
      (bucket-delete
       [this k]
       (when-let [resp (exec-req [k] {:method :delete})]
	 resp))	  
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