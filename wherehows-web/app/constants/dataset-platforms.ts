/**
 * The known/supported list of dataset platforms
 * @enum {string}
 */
enum DatasetPlatform {
  Kafka = 'KAFKA',
  Espresso = 'ESPRESSO',
  Oracle = 'ORACLE',
  MySql = 'MYSQL',
  Teradata = 'TERADATA',
  HDFS = 'HDFS',
  GreenPlum = 'GREENPLUM',
  S3 = 'S3'
}

export { DatasetPlatform };
