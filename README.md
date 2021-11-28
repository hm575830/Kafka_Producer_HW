# Kafka Homework
## 參數
--topic: 目標topic\
--records: 資料筆數\
--recordSize: 每一筆資料的大小
## 資料內容規定:

key必須為字串格式，內容為"key-第幾筆的編號"，例如"key-000000"\
value必須為字串格式，內容為"value-第幾筆的編號"，例如"value-000000"\
換言之，key and value都是唯一！！！
