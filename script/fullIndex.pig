--load from S3 Storage using wild card to read all file under the directory
--create inverted index for all.  (term,times,docId)

relation = LOAD 's3://trevii/NEU_201209/courses/MapReduce/homeworks/final_project/data/inverted_index/enwiki-20121001-pages-articles1.xml-p000000010p000010000.rst' as (docId:long, term:chararray, times:long);

transform_column = FOREACH relation GENERATE term, times, docId;

filtered = FILTER transform_column BY SIZE(term) > 2;

ordered = ORDER filtered BY term, times desc PARALLEL 30;

groupby_docId = GROUP relation BY docId;
stat = FOREACH groupby_docId { totalwords = COUNT(relation);  totaltimes = SUM(relation.times); GENERATE group as docId, totalwords as totalwords, totaltimes as totaltimes;};

order_stat_by_total_word = ORDER stat by totalwords desc;
order_stat_by_total_times = ORDER stat by totaltimes desc;
 

STORE ordered into 's3://finalgongcheng/fullIndex';
STORE order_stat_by_total_word into 's3://finalgongcheng/stat_word';
STORE order_stat_by_total_times into 's3://finalgongcheng/stat_times';
