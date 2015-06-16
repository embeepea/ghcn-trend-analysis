(ns ghcn.work1
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [incanter.stats :as stats]
            [ghcn.core :as core]))

(use 'ghcn.core)

; The `use` function above arranges for core.clj to be executed
; and all its definitions to be available here.  This includes:
;
;   stations-meta
;       a map whose keys are stations ids and whose values give
;       various station metadata; see the comments in core.clj
;       for details.
;   stations-por-summary:
;       a map whose keys are stations ids and whose values give
;       period-of-record summary information for the stations;
;       this data is read from the file "data/stations/summary.edn.txt"
;       which is an edn translation of "data/stations/summary.json"
;       which is created by geogaddi. stations-por-summary looks
;       like this:
;           {
;             "USC00414770" {
;               :tmax {:min "19880405",:max "20150525",:state 3},
;               :snow {:min "19880405",:max "20150525",:state 3},
;               :prcpytd {:min "19880101",:max "20150527",:state 0},
;               :prcp {:min "19880405",:max "20150525",:state 3},
;               :tmin {:min "19880405",:max "20150525",:state 3}
;             },
;             "USC00389122" {
;               :snow {:min "19650701",:max "20150226",:state 2},
;               :prcpytd {:min "19650101",:max "20150527",:state 0},
;               :prcp {:min "19650701",:max "20150525",:state 1}
;             }
;             ...
;           }
;       (At the moment I don't remember what the :state field means.)



(def station-ids-covering-1950-2010
  (keys (filter
         (fn [[k v]]
           (and
            (contains? v :tmin)
            (contains? v :tmax)
            (<= (compare (get-in v [:tmin :min]) "19500000") 0)
            (>= (compare (get-in v [:tmin :max]) "20100000") 0)
            (<= (compare (get-in v [:tmax :min]) "19500000") 0)
            (>= (compare (get-in v [:tmax :max]) "20100000") 0)
            ))
         stations-por-summary)))

; station-ids-covering-1950-2010 is now a list of the ids of stations whose POR
; for both TMIN and TMAX is at least 1950 through 2010


; The following computes a tmin/tmax data fraction map for the period
; 1950-2010; keys are station ids, values are maps with keys :tmin
; and :tmax giving the corresponding data fractions for the period.
; This takes a while to compute, so the following code is commented
; out and the results are read from a saved file below instead.
#_(def station-ids-covering-1950-2010-data-fraction-map
  (into {}
        (for [id station-ids-covering-1950-2010]
          (do
            (println id)
            [id {:tmin (data-fraction id "TMIN" 1950 2010)
                 :tmax (data-fraction id "TMAX" 1950 2010)}]
            ))))
; The following write the above to a file
#_(spit "data/stations/station-ids-covering-1950-2010-data-fraction-map.edn.txt"
      (with-out-str (pr station-ids-covering-1950-2010-data-fraction-map)))

; Here we load the contents of the file:
(def station-ids-covering-1950-2010-data-fraction-map
  (load-edn "data/stations/station-ids-covering-1950-2010-data-fraction-map.edn.txt"))

; Set high-fraction-station-ids-covering-1950-2010 to the list of stations from
; station-ids-covering-1950-2010-data-fraction-map having both :tmin and :tmax
; data fractions of at least 0.9
(def high-fraction-station-ids-covering-1950-2010
  (doall (map first
              (filter
               (fn [[id fracs]] (and (>= (:tmax fracs) 0.9) (>= (:tmin fracs) 0.9)))
               station-ids-covering-1950-2010-data-fraction-map))))

; Set good-year-map to a map whose keys are station ids, and whose values
; are vectors giving the years for which each station has a stretch of
; at most 7 consecutive days of missing TMAX or TMIN data.  This takes
; a while to run, so it's commented out and we read the pre-computed
; results from a file below.
#_(def good-year-map
  (into {}
        (for [id high-fraction-station-ids-covering-1950-2010]
          (let [years      (clojure.set/intersection
                            (set (core/good-years id "TMAX" 7))
                            (set (core/good-years id "TMIN" 7)))]
            (println id)
            [id (vec (sort years))]))))
; The following writes the above to a file
#_(spit "data/work1/good-year-map.edn.txt" (with-out-str (pr good-year-map)))
; Here we load the contents of the file:
(def good-year-map (load-edn "data/work1/good-year-map.edn.txt"))

; Set stations-data-trend-map to a map whose structions is like this:
; {"USC00305597" {:tmax {:data [[1949 59.70] [1950 57.38] ...]
;                        :trend [1.1484 0.02866]}
;                 :tmin {:data [[1949 49.70] [1950 47.38] ...]
;                        :trend [1.0381 0.02165]},
;                 :tavg {:data [[1949 49.70] [1950 47.38] ...]
;                        :trend [1.0381 0.02165]}},
;  ...
; }
;
#_(def stations-data-trend-map
  (doall (into {}
               (for [id high-fraction-station-ids-covering-1950-2010]
                 (let [years           (good-year-map id)
                       tmax-data-map   (load-data-year-map id "TMAX")
                       tmin-data-map   (load-data-year-map id "TMIN")
                       tavg-data-map   (load-data-year-map id "TAVG")
                       year-avgs       (fn [data-map]
                                         (sort-by
                                          (fn [[year val]] year)
                                          (map
                                           (fn [year] [year (scale-temp (avg (non-nils (data-map year))))])
                                           years)))
                       tmax-year-avgs  (year-avgs tmax-data-map)
                       tmin-year-avgs  (year-avgs tmin-data-map)
                       tavg-year-avgs  (year-avgs tavg-data-map)
                       data-trend-map  (fn [data]
                                         {:data (vec data)
                                          :trend (:coefs (apply stats/linear-model (split-coords data)))})
                       ]
                   (println id)
                   [id {:tmax (data-trend-map tmax-year-avgs)
                        :tmin (data-trend-map tmin-year-avgs)
                        :tavg (data-trend-map tavg-year-avgs)}]
                   )))))


; The following writes the above to a file
#_(spit "data/work1/stations-data-trend-map.edn.txt" (with-out-str (pr stations-data-trend-map)))

; Here we load the contents of the file:
(def stations-data-trend-map (load-edn "data/work1/stations-data-trend-map.edn.txt"))

#_(spit "data/work1/stations-data-trend-map.json"
      (json/write-str 
       (map
        (fn [[id m]] (assoc m :id id :latlon (core/station-latlon id) :name (core/station-state-name id)))
        stations-data-trend-map)
       ))

#_(def us-station-ids (filter in-us? (keys stations-meta)))
#_(def us-station-map
  (map
   (fn [id]
     (let [station (stations-meta id)]
       { :id id
        :latlon (station-latlon id)
        :name (str (:state station) " " (:name station)) }))
   us-station-ids))
#_(spit "data/stations/us-station-map.json" (json/write-str us-station-map))


"work1.clj"
