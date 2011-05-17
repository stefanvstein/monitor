(ns jmeter.rps
  (:require [clj-http.client :as client])
  (:use (monitor tools))
  (:import (java.text SimpleDateFormat))
  (:import [java.io FileWriter])
  (:import (com.csvreader CsvReader)))

#_(def printex (fn [& _]))
(def printex println)

(defn- record-seq [filename]
  (let [csv (CsvReader. filename)
        read-record (fn [] 
                      (when (.readRecord csv) 
                        (into [] (.getValues csv))))]
    (take-while (complement nil?) (repeatedly read-record))))

(defn csv-seq
  "Return a lazy sequence of records (maps) from CSV file.

  With header map will be header->value, otherwise it'll be position->value."
  ([filename] (csv-seq filename false))
  ([filename headers?]
   (let [records (record-seq filename)
         headers (if headers? (first records) (range (count (first records))))]
     (map #(zipmap headers %) (if headers? (rest records) records)))))

(defn pool [e]
  (get e "pool"))

(defn from [e]
  (get e "from"))

(defn to [e]
  (get e "to"))

(defn train-no [e]
  (get e "train"))

(defn vehicles
  ([return-f filter-f]
     (distinct (map return-f (filter filter-f (csv-seq "vehicles.csv" true)))))
  ([]
     (vehicles identity identity)))

(defn trains
  ([return-f filter-f]
     (distinct (map return-f (filter filter-f (csv-seq "trains.csv" true)))))
  ([]
     (vehicles identity identity)))


(defn fordon? [e]
  (= "Fordon" (get e "category")))

(defn post [host port body]
    (client/post (str "http://" host ":" port "/plancom/")
                 {:body body
                  :headers {"SoapAction" "\"\""}}))

(defn rotation [host port pool start stop]
  (let [body (str "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:veh=\"http://www.qnamic.com/schema/request/report/vehiclerotation\" xmlns:com=\"http://www.qnamic.com/schema/common\">    <soapenv:Header/>    <soapenv:Body>       <veh:Report>          <veh:VehiclePoolId>" pool "</veh:VehiclePoolId>          <veh:StartDate>" start "</veh:StartDate>          <veh:EndDate>" stop "</veh:EndDate>       </veh:Report>    </soapenv:Body> </soapenv:Envelope>")]
    (post host port body)))

(defn rotations [host port pools days]
  (for [pool pools day days]
    (try
      (when (rotation host port pool day day)
        (println day pool))
      (catch Exception e
        (printex (.getMessage e))
        nil))))

(defn coachlist [host port train day station]
  (let [body "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:coac=\"http://www.qnamic.com/schema/request/report/coachlist\">
   <soapenv:Header/>
   <soapenv:Body>
      <coac:Report>
         <coac:TrainNumber>" train "</coac:TrainNumber>
         <coac:Date>" day "</coac:Date>
         <coac:Station>" station "</coac:Station>
      </coac:Report>
   </soapenv:Body>
</soapenv:Envelope>"]
    (post host port body)))

(defn departure [host port train date station] 
  (let [body (str "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:dep=\"http://www.qnamic.com/schema/request/report/composition/departing\">    <soapenv:Header/>    <soapenv:Body>       <dep:Report>          <dep:TrainNumber>" train "</dep:TrainNumber>          <dep:Date>" date "</dep:Date>          <dep:Station>" (if station station "") "</dep:Station>       </dep:Report>    </soapenv:Body> </soapenv:Envelope>")]
    (post host port body)))

(defn departures [host port trains days stations]
  (for [train trains day days station stations]
    (try
      (when (departure host port train day station)
        (println day train station))
      (catch Exception e
        (printex (.getMessage e))
        nil))))

(defn arrival [host port train date station]
  (let [body (str "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:arr=\"http://www.qnamic.com/schema/request/report/composition/arrival\">    <soapenv:Header/>    <soapenv:Body>       <arr:Report>          <arr:TrainNumber>" train "</arr:TrainNumber>          <arr:Date>" date "</arr:Date>          <arr:Station>" (if station station "") "</arr:Station>       </arr:Report>    </soapenv:Body> </soapenv:Envelope>")]
  (post host port body)))

(defn arrivals [host port trains days stations]
  (for [train trains day days station stations]
    (try
      (when (arrival host port train day station)
        (println day train station))
      (catch Exception e
        (printex (.getMessage e))
        nil))))


(defn turnaround [host port pool start stop station]
  (let [body (str "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tur=\"http://www.qnamic.com/schema/request/report/turnaround\" xmlns:com=\"http://www.qnamic.com/schema/common\">    <soapenv:Header/>    <soapenv:Body>       <tur:Report>          <tur:VehiclePoolId>" pool "</tur:VehiclePoolId>          <tur:StartDate>" start "</tur:StartDate>          <tur:EndDate>" stop "</tur:EndDate>          <tur:Station>" station "</tur:Station>       </tur:Report>    </soapenv:Body> </soapenv:Envelope>")]
(post host port body)))

(defn turnarounds [host port pools days stations]
  (for [pool pools day days station stations]
    (try
      (when (turnaround host port pool day day station)
        (println day day pool station))
      (catch Exception e
        (printex (.getMessage e))
        nil))))

(defn depot-trains [host port depot start stop]
  (let [body (str "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:dep=\"http://www.qnamic.com/schema/request/depottrain\">
   <soapenv:Header/>
   <soapenv:Body>
      <dep:DepotTrains>
         <dep:DepotName>" depot "</dep:DepotName>
         <dep:StartDate>" start "</dep:StartDate>
         <dep:EndDate>" stop "</dep:EndDate>
      </dep:DepotTrains>
   </soapenv:Body>
</soapenv:Envelope>")]
    (post host port body)))

(defn inspection-interval [host port vehicle-id vehicle-type]
  (let [body (str "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:imp=\"http://www.qnamic.com/schema/request/imp\" xmlns:com=\"http://www.qnamic.com/schema/common\" xmlns:common=\"http://www.qnamic.com/schema/common\">
   <soapenv:Header/>
   <soapenv:Body>
      <imp:MasterDataImport type=\"INSPECTION_INTERVAL\">
         <imp:Resource id=\"prestandatest\" updateMode=\"update\">
            <imp:ValidFrom timestamp=\"2008-03-11T16:30:00\">
         <imp:Attribute key=\"INSPECTION_TYPE\" value=\"T1\"/>
         <imp:Attribute key=\"NORMATIVE_KM_LIMIT\" value=\"200000\"/>
         <imp:Attribute key=\"UPPER_KM_LIMIT\" value=\"2300000\"/>
         <imp:Attribute key=\"NORMATIVE_DATE_LIMIT\" value=\"2008-03-11T16:30:00\"/>
         <imp:Attribute key=\"UPPER_DATE_LIMIT\" value=\"2008-10-11T16:30:00\"/>
         <imp:Attribute key=\"NORMATIVE_OPERATING_HOURS_LIMIT\" value=\"12\"/>
         <imp:Attribute key=\"UPPER_OPERATING_HOURS_LIMIT\" value=\"14\"/>
         <imp:Attribute key=\"MILEAGE_AT_LAST_INSPECTION\" value=\"11000\"/>
         <imp:Attribute key=\"DATE_AT_LAST_INSPECTION\" value=\"2008-02-11T16:30:00\"/>
         <imp:Attribute key=\"OPERATING_HOURS_AT_LAST_INSPECTION\" value=\"23000\"/>
                <imp:CustomerAttributes>
                   <imp:CustomerAttribute>
                      <com:VehicleId>" vehicle-id "</com:VehicleId>
                      <com:VehicleType>" vehicle-type "</com:VehicleType>
                      <com:EuVehicleId></com:EuVehicleId>
                   </imp:CustomerAttribute>
                </imp:CustomerAttributes>
            </imp:ValidFrom>
         </imp:Resource>
      </imp:MasterDataImport>
   </soapenv:Body>
</soapenv:Envelope>")]
    (post host port body)))

(defn pda [host port train date]
  (let [body (str "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tra=\"http://www.qnamic.com/schema/request/traincomposition\">
   <soapenv:Header/>
   <soapenv:Body>
      <tra:TrainCompositions>
         <tra:TrainComposition>
            <tra:TrainNumber>" train "</tra:TrainNumber>
            <tra:Date>" date "</tra:Date>
         </tra:TrainComposition>
      </tra:TrainCompositions>
   </soapenv:Body>
</soapenv:Envelope>")]
    (post host port body)))

(defn pdas [host port trains days]
  (for [train trains day days]
    (try
      (when (pda host port train day )
        (println day train))
      (catch Exception e
        (printex (.getMessage e))
        nil))))

(defn date [string]
  (.parse (SimpleDateFormat. "yyyy-MM-dd") string))

(defn days [from to]
  (let [df (SimpleDateFormat. "yyyy-MM-dd")]
    (map #(.format df %) (take-while #(< (.getTime %) (.getTime to)) (day-by-day from)))))

(defn test-turnarounds [days]
  (let [pools ["X2_5"]
        stations ["HGL"]]
    (dorun (turnarounds "litmsj610.sj.se" 34000 pools days stations))))

(defn test-all-turnarounds [days]
  (let [pools (vehicles pool fordon?)
        stations (trains from identity)]
    (dorun (turnarounds "litmsj610.sj.se" 34000 (take 10 pools) days (take 10 stations)))))

(defn test-all-departures [days]
  (let [train-and-from (fn [e] [(get e "train") (get e "from")])
        tra (trains train-and-from identity)]
    (dorun (departures "litmsj623.sj.se" 34000 (take 1 (map first tra)) days (take 1 (map second tra))))))

(defn test-all-rotations [days]
  (let [pools (vehicles pool fordon?)]
    (dorun (rotations "litmsj610.sj.se" 34000 (take 10 pools) days))))

(defn test-all-pdas [days]
  (let [train (take 10 (trains train-no identity))]
    (dorun (pdas "litmsj623.sj.se" 34000 train days))))

(defn fetch-all [days host port]
  (future
    (let [train (trains train-no identity)]
      (with-open [fw (FileWriter. "pda.csv")]
        (binding [*out* fw]
          (dorun (pdas host port train days))))))
  (future
    (let [pools (vehicles pool fordon?)]
       (with-open [fw (FileWriter. "rotations.csv")]
         (binding [*out* fw]
           (dorun (rotations host port pools days))))))
  (future
    (let [train-and-from (fn [e] [(get e "train") (get e "from")])
          tra (trains train-and-from identity)]
      (with-open [fw (FileWriter. "departures.csv")]
        (binding [*out* fw]
          (dorun (departures host port (map first tra) days (map second tra)))))))
  
  (future
    (let [train-and-to (fn [e] [(get e "train") (get e "to")])
          tra (trains train-and-to identity)]
      (with-open [fw (FileWriter. "arrivals.csv")]
        (binding [*out* fw]
          (dorun (arrivals host port (map first tra) days (map second tra)))))))
  
  (future
    (let [pools (vehicles pool fordon?)
          stations (trains from identity)]
      (with-open [fw (FileWriter. "turnarounds.csv")]
        (binding [*out* fw]
          (dorun (turnarounds host port pools days stations))))))
  
  (future
    (let [depots ["HGL" "G"]]
      (with-open [fw (FileWriter. "depottrains.csv")]
        (binding [*out* fw]
          (dorun (depot-trains host port depots days days))))))
  
)
