(ns store.riak
  (:use store.api)
  (:require [clj-http.client :as client]
	    [ring.util.codec :as ring]
	    [clj-json.core :as json]))

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
    (reify store.api.IReadBucket
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
                clojure.string/join
                java.io.StringReader.
                java.io.BufferedReader.
                json/parsed-seq
                ((partial apply merge-with concat))
                (get "keys")
                ((partial map ring/url-decode))))
           (bucket-exists?
            [this k]
            (default-bucket-exists? this k))

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