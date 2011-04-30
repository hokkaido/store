(ns store.riak
  (:require [fetcher.core :as client]
            [ring.util.codec :as ring]
            [clojure.string :as str]
            [clj-json.core :as json]
            [clj-time.coerce :as time-coerce])
  (:use store.core
       plumbing.core
        plumbing.error
        plumbing.streams)
  (:require [clojure.contrib.logging :as log])
  (:import java.text.SimpleDateFormat
           org.joda.time.DateTime))

(defn decode-chunked-objs [os]
  (->> os
       (map #(get % "keys"))
       (remove empty?)
       flat-iter
       iterator-seq
       (map ring/url-decode)))

(defn ^DateTime parse-rfc2822 [^String d]
  (time-coerce/from-date
   (.parse
      (SimpleDateFormat. "EEE, dd MMM yyyy HH:mm:ss Z")
      d)))

(defn last-modified [resp]
  (when-let [d (-> resp
                 :headers
                 (get "last-modified"))]
    (try (parse-rfc2822 d)
	 (catch java.lang.Exception _ nil))))

(defn process-keys-resp [body]
  (-> body
      (java.io.InputStreamReader.)
      json/parsed-seq
      decode-chunked-objs))

(defn handle-riak-resp [process-resp {:keys [status,body,url] :as resp}]
  (case status
   (200 300 304 204) (process-resp resp)
   (404 400 503) (throw (RuntimeException.
			 (format "Unable to satisfy request %s" resp)))))

(defn get-riak-json-body [o]
  {:body (json/generate-string o)
   :content-type "application/json" :accepts :json})

(defn get-riak-req-url [{:keys [server,name,port,prefix]
			 :or {server "http://localhost"
			      port 8098
			      prefix "riak"}}
			path-args]
  (assert name)
  (let [req-base [(str server ":" port) prefix (ring/url-encode name)]]
    (->> path-args
	 (map (comp ring/url-encode str))
	 (concat req-base)
	 (str/join "/"))))

(defn exec-riak-req
  [get-client riak-opts path-args
   method req-opts
   process-body]  
  (let [url (get-riak-req-url riak-opts path-args)
	req (-> req-opts
		(assoc :url url)
		(update-in [:body]
		  (fn [^String b] (when b (.getBytes b "UTF8")))))
	resp (fetcher.core/fetch get-client method req)]
    (handle-riak-resp process-body resp)))

(defn riak-bucket
  "opts consists of
            :server (default \"http://localhost\")
            :port (default \"8098\")
            :prefix (default \"riak\")
   You must provie :name argument for the bucket name"
  [& {:keys [observer,keywordize?,get-client]
      :or {observer (constantly nil)
	   get-client fetcher.core/basic-http-client}
      :as riak-opts}]
  (let [exec (partial with-ex observer
		exec-riak-req get-client riak-opts)] 	       
   (reify
    store.core.IReadBucket   
    (bucket-get [this k]    
		(let [res (exec [k] :get nil (comp json/parse-string :body))]
		  (if keywordize?
		    (keywordize-map res)
		    res)))   
    (bucket-seq [this]
		(default-bucket-seq this))        
    (bucket-keys [this]
		 (exec
		  [] :get {:query-params {"keys" "stream"}
			   :as :input-stream
			   :no-gzip? true}
		  (comp process-keys-resp :body)))
    (bucket-exists?
     [this k]
     (default-bucket-exists? this k))
    (bucket-modified
     [this k]
     (exec [k] :head nil last-modified))
   
    store.core.IWriteBucket
    (bucket-put
     [this k v]
     (exec [k] :post (get-riak-json-body v) identity))  
    (bucket-delete
     [this k]
     (exec [k] :delete nil identity))	  
    (bucket-update
     [this k f]
     (default-bucket-update this k f))
    (bucket-sync [this] nil)
    (bucket-close [this] nil))))