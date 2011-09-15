(ns store.api
  (:refer-clojure :exclude [remove])
  (:use [plumbing.core :only [?> ?>> map-from-vals map-map]]
	[clojure.java.io :only [file]]
	[plumbing.error :only [with-ex logger]]
	store.net
	store.bdb
        store.s3)
  (:require [plumbing.observer :as obs]
	    [store.core :as bucket])
  (:import [java.util.concurrent Executors TimeUnit
	    ConcurrentHashMap]))

(declare bucket-ops store-op)

(defprotocol IStore
  (exec [this fn-form] "execute a fn on the server, giving the store as the only argument.")
  (buckets [this _] "fetcha list of buckets.")
  (bucket [this k] "fetch a bucket")
  (add [this k] "add a bucket using the stores context")
  (remove [this k] "remove a bucket"))

(deftype Store [bucket-map dispatch context]
  IStore
  (exec [this form] ((eval form) this))
  (bucket [this bucket-name] (bucket/get bucket-map bucket-name))
  (buckets [this _] (bucket/keys bucket-map))
  (add [this bucket-name] (let [bucket (bucket/create-buckets (assoc context
							 :name bucket-name))]
			    (bucket/put
			     bucket-map
			     bucket-name
			     bucket)))
  (remove [this bucket-name] (bucket/delete bucket-map bucket-name))
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

(def bucket-ops
     {:exec exec
      :buckets buckets
      :bucket bucket
      :add add
      :remove remove})

(defn store-op [^store.api.Store store op & args]
  (let [{:keys [type]} (.context store)
        name (first args)
	args (rest args)]
    (cond
     ;;should just send fn.  over rest, it gets quoted and then evaled on server.
     (= op :eval) ((eval name) store)
     (find bucket-ops op)
     (let [local ((op bucket-ops) store name)]
       (if-not (= type :rest) local
               ((op rest-bucket-ops) store name)))
     :else
     (let [read (bucket/read-ops op)
	   spec (->> name (bucket/get (.bucket-map store)))
	   b (if read (:read spec)
		 (:write spec))
	   f (or read (bucket/write-ops op))]
       (when-not b
	 (when-not spec
	   (throw (Exception. (format "No bucket %s" name))))
	 (let [read-or-write (if read "read" "write")]
	   (throw (Exception. (format "No %s operation for bucket %s" read-or-write name)))))
       (apply f b args)))))

;;TODO: fucked, create a coherent model for store flush and shutdown
(defn flush! [^Store store]
  (doseq [[_ spec] (bucket/seq (.bucket-map store))
	  :when (-> spec :write-spec)]
    (bucket/sync (:write spec))))

(defn shutdown [^Store store]
  (doseq [[name spec] (bucket/seq (.bucket-map store))
	  f (:shutdown spec)]
    (with-ex (logger) f))
  (doseq [[name spec] (bucket/seq (.bucket-map store))]
    (with-ex (logger)  bucket/close (:read spec))
    (with-ex (logger)  bucket/close (:write spec))))

(defn start-flush-pools [bucket-map]
  (->> bucket-map
       bucket/seq
       (map-map
	(fn [{:keys [write,write-spec] :as bucket-spec}]
	  (let [{:keys [flush-freq,num-flush-threads]} write-spec]
	    (if-not flush-freq
	      bucket-spec
	      (let [pool (doto (Executors/newSingleThreadScheduledExecutor)
			   (.scheduleAtFixedRate			  
			    #(with-ex (logger) bucket/sync write)
			    (long 0) (long flush-freq)
			    TimeUnit/SECONDS))]
		(-> bucket-spec
		    (assoc :flush-pool pool)
		    (update-in [:shutdown]
			       conj
			       (fn []
				 (bucket/sync write)
				 (.shutdownNow pool)))))))))
       doall
       ^java.util.Map (into {})
       (ConcurrentHashMap.)
       bucket/hashmap-bucket))

(defn observe-merge [ks old v]
  (if old (merge-with + old v) v))

(defn observe-report [m duration]
  (map-map
   (fn [{:keys [queue-size] :as b}]
     (let [{:keys [count size]} queue-size]
       (if (and count (> count 0))
         (assoc b :queue-size (/ size 1.0 count))
         b)))
   m))

(defn store [bucket-specs & [context]]
  (let [context (update-in (or context {}) [:observer]
                           obs/sub-observer (obs/gen-key "store"))]
    (-> (bucket/buckets bucket-specs context)
	start-flush-pools
	(Store.
	 (obs/observed-fn
	    (:observer context) :counts
	    {:type :counts :group (obs/observed-fn
				   (:observer context) :counts
				   {:type :counts :group (fn [[_ op b]] [b op])}
				   store-op)}
	    store-op)
	 context))))

(defn clone [^store.api.Store s & [context]]
  (store (bucket/keys (.bucket-map s)) context))

(defn mirror-remote [spec]
  (let [s (store [] spec)
	ks (s :buckets)]
    (store ks spec)))

(defn copy [in out bucket & {:keys [select skip]  ;;skip to skip keys
			     :or {select identity ;;select to select values
}}]
  (doseq [[k v] (->> (in :seq bucket)
		     (?>> skip filter (comp skip first)))
	  :let [vs (select v)]]
    (out :put bucket k vs))
  (out :sync bucket))

(defn sync-stores [in out bucket & args]
  (apply copy in out bucket
	 (concat args [:skip
		       (complement (partial out :get bucket))])))

(defn merge-stores [host other]
  (doseq [[k v] (bucket/seq (.bucket-map other))]
    (bucket/put (.bucket-map host) k v))
  host)