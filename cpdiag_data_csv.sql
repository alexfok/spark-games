use cpdiag.tmp;
ALTER SESSION SET `store.format`='csv';
create table cpdiag.tmp.cpdiag_data_csv as select * from `cpdiag`.`user/mapr/cp_out/current/cpdiag_data.parquet`;


