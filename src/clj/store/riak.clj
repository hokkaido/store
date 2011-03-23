(ns store.riak
  (:require [fetcher.client :as client]
	    [fetcher.core :as fetcher-core]
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

(defn process-keys-resp [body]
  (decode-chunked-objs
   (if (string? body)
     (->> body (java.io.StringReader.) json/parsed-seq)
     (->> body (map json/parse-string)))))

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
		   (fn [args method req-opts & [no-gzip?]]
		     (apply
		      client/request
		      #(fetcher-core/basic-http-client)
		      method
		      (assoc req-opts
			:url (->> args
				  (map (comp ring/url-encode str))
				  (apply mk-path)))
		      (when no-gzip? [:accept-encoding nil]))))
	read-resp (with-log :error (comp json/parse-string :body))]
    ;; IBucket Implementatin
    (reify
      store.api.IReadBucket
      (bucket-get
       [this k]
       (when-let [resp (exec-req [k] :get nil)]
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
       (let [read-keys-resp (with-silent
			      (comp
			       process-keys-resp
			       :body
			       (fn [req]
				 (exec-req [] :get req true))))]
	 (read-keys-resp
	  {:query-params {"keys" "stream"}})))
      (bucket-exists?
       [this k]
       (default-bucket-exists? this k))
      (bucket-modified
       [this k]
       (when-let [resp (exec-req [k] :head nil)]
	 (last-modified resp)))
      store.api.IWriteBucket
      (bucket-put
       [this k v]
       (when-let [resp (exec-req [k] :post  (mk-json v))]
	 resp))  
      (bucket-delete
       [this k]
       (when-let [resp (exec-req [k] :delete nil)]
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