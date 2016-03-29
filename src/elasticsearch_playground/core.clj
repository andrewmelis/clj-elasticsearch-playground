(ns elasticsearch-playground.core
  (:require [clojurewerkz.elastisch.rest          :as esr]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.query         :as q]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojurewerkz.elastisch.aggregation   :as a]
            [clojure.pprint :as pp]))


(defn base-query
  "pass in a query map"
  [query]
  (let [conn (esr/connect "http://your.ip.here:9200")
        res (esd/search-all-types conn
                                  "_all"
                                  :query query
                                  :size 50)
        n    (esrsp/total-hits res)
        hits (esrsp/hits-from res)]
    (assoc {}
           :number-of-hits n
           :hits hits
           :raw-response res)))

(defn example-2 []
  (let [conn (esr/connect "http://your.ip.here:9200")
        res (esd/search-all-types conn
                                  "_all"
                                  :query (q/bool {:must [(q/term :slug "starbucks")
                                                         (q/term (keyword "@log_type") "event")
                                                         (q/term :event  "product.sold")]})
                                  ;; :aggregations {:test-agg (a/avg {:script "Float.parseFloat(doc['data.brand'].value)"})}
                                  :aggregations {:test-agg {:avg {:script "Float.parseFloat(doc['data.buyer_id'].value)"}}}
                                  :size 5)
        n    (esrsp/total-hits res)
        ;; agg (esrsp/aggregation-from res :test-agg)
        hits (esrsp/hits-from res)]
    (assoc {}
           :number-of-hits n
           :hits hits
           ;; :agg agg
           :raw-response res)))

;; e83a183d-de1d-4b1a-b831-f20acf537d86
(defn example-3
  "simple term query"
  []
  (let [user-id "e83a183d-de1d-4b1a-b831-f20acf537d86"
        base (base-query (q/match :user_id user-id))
        hits (:hits base)]
    (->> hits
         (remove #(= "session-infos" (get-in % [:_source :data :params :controller])))
         (remove #(not= user-id (get-in % [:_source :data :user_id])))
         pp/pprint)))


(defn example-4
  "bool query wrapping a couple of example queries"
  []
  (let [query (q/bool {:must [(q/term :slug "starbucks")
                              (q/term (keyword "@log_type") "event")
                              (q/term :event  "product.sold")]})]
    (-> query
        base-query
        :hits
        pp/pprint)))
