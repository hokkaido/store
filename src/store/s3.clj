(ns store.s3
  (:use [plumbing.core :only [?>]])
  (:require [clojure.contrib.duck-streams :as ds]
	    [store.core :as bucket])
  (:import
   java.io.File
   [java.io DataOutputStream ByteArrayOutputStream ObjectOutputStream
    DataInputStream ByteArrayInputStream ObjectInputStream]
   org.jets3t.service.S3Service
   org.jets3t.service.impl.rest.httpclient.RestS3Service
   org.jets3t.service.model.S3Object
   org.jets3t.service.security.AWSCredentials
   org.jets3t.service.utils.ServiceUtils))

(defn s3-connection 
  ([{access-key :key secret-key :secretkey}] 
     (s3-connection access-key secret-key))
  ([k sk] (RestS3Service. (AWSCredentials. k sk))))

(defn s3-buckets [s3] (.listAllBuckets s3))

(defn objects
  ([s3 bucket-name] 
     (.listObjects s3 (.getBucket s3 bucket-name)))
  ([s3 bucket-root rest] 
     (.listObjects s3 (.getBucket s3 bucket-root) rest nil)))

(defn get-keys [s b]
  (map #(.getKey %)
       (seq (objects s b))))


(defn create-bucket [^RestS3Service s3 ^String bucket-name] 
  (.createBucket s3 bucket-name))

(defn delete-bucket [^RestS3Service s3 ^String bucket-name]
  (.deleteBucket s3 bucket-name))

(defn delete-object [^RestS3Service s3 ^String bucket-name ^String key]
  ;; In case the J3tset API changes and starts returning non-nil on success.
  ;; NOTE: S3 seems to be returning nil regardless of whether key existed.
  ;; In that case, this function always returns success even if key didn't exist.
  (or (.deleteObject s3 bucket-name key)
      :success))


(defn put-clj [^RestS3Service s3 ^String bucket-name ^String key clj]
  (let [bucket (.getBucket s3 bucket-name)  
        s3-object (S3Object. bucket key ^String (pr-str clj))]
    (.putObject s3 bucket s3-object)))

(defn get-clj [^RestS3Service s3 ^String bucket-name ^String key]
  (let [bucket (.getBucket s3 bucket-name)
        obj (.getObject s3 bucket key)
        s   (.getDataInputStream obj)]
    (when s (read (java.io.PushbackReader. (java.io.InputStreamReader. s))))))

(defmethod bucket/bucket :s3
  [{:keys [prefix merge name] :as args}]
  (let [s3 (s3-connection args)
	bucket-name (str prefix name)]
    (create-bucket s3 bucket-name)
    (->
     (reify bucket/IReadBucket
	    (bucket/keys [this]
			 (get-keys s3 bucket-name))
	    (bucket/get [this k]
			(get-clj s3 bucket-name (str k)))
	    (bucket/exists? [this k]
			    (some #(= k (.getKey %))
				  (-> s3 (objects bucket-name (str k)) seq)))
	    (bucket/seq [this] (bucket/default-seq this))	 
	    (bucket/batch-get [this ks] (throw (UnsupportedOperationException.)))
	    (bucket/count [this] (throw (UnsupportedOperationException.)))

	    bucket/IMergeBucket
	    (bucket/merge [this k v]
			  (bucket/default-merge this merge k v))

	    
	    bucket/IWriteBucket
	    (bucket/put [this k v]
			(put-clj s3 bucket-name (str k) v))
    
	    (bucket/delete [this k]
			   (delete-object s3 bucket-name (str k)))
    
	    (bucket/update [this k f]
			   (bucket/default-update this k f))
	    (bucket/close [this] (throw (UnsupportedOperationException.)))
	    (bucket/sync [this] (throw (UnsupportedOperationException.))))
     (bucket/wrapper-policy args))))