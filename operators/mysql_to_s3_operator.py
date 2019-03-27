from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from mysql_plugin.hooks.astro_mysql_hook import AstroMySqlHook

from airflow.utils.decorators import apply_defaults
import json
import logging


class MySQLToS3Operator(BaseOperator):
    """
    MySQL to Spreadsheet Operator

    NOTE: When using the MySQLToS3Operator, it is necessary to set the cursor
    to "dictcursor" in the MySQL connection settings within "Extra"
    (e.g.{"cursor":"dictcursor"}). To avoid invalid characters, it is also
    recommended to specify the character encoding (e.g {"charset":"utf8"}).

    NOTE: Because this operator accesses a single database via concurrent
    connections, it is advised that a connection pool be used to control
    requests. - https://airflow.incubator.apache.org/concepts.html#pools

    :param mysql_conn_id:           The input mysql connection id.
    :type mysql_conn_id:            string
    :param mysql_query:             The input MySQL query to pull data from.
    :type mysql_query:              string
    :param mysql_params:            The MySQL parameters for the query.
    :type mysql_params:             string
    :param s3_conn_id:              The destination s3 connection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The destination s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The destination s3 key.
    :type s3_key:                   string
    :param package_schema:          *(optional)* Whether or not to pull the
                                    schema information for the table as well as
                                    the data.
    :type package_schema:           boolean
    :param incremental_key:         *(optional)* The incrementing key to filter
                                    the source data with. Currently only
                                    accepts a column with type of timestamp.
    :type incremental_key:          string
    :param start:                   *(optional)* The start date to filter
                                    records with based on the incremental_key.
                                    Only required if using the incremental_key
                                    field.
    :type start:                    timestamp (YYYY-MM-DD HH:MM:SS)
    :param end:                     *(optional)* The end date to filter
                                    records with based on the incremental_key.
                                    Only required if using the incremental_key
                                    field.
    :type end:                       timestamp (YYYY-MM-DD HH:MM:SS)
    """

    template_fields = ['start', 'end', 's3_key']

    @apply_defaults
    def __init__(self,
                 mysql_conn_id,
                 mysql_query,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 mysql_params=None,
                 package_schema=False,
                 incremental_key=None,
                 start=None,
                 end=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.mysql_query = mysql_query
        self.mysql_params = mysql_params
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.package_schema = package_schema
        self.incremental_key = incremental_key
        self.start = start
        self.end = end

    def execute(self, context):
        hook = AstroMySqlHook(self.mysql_conn_id)
        self.get_records(hook)
        if self.package_schema:
            self.get_schema(hook, self.mysql_table)

    def get_schema(self, hook, table):
        logging.info('Initiating schema retrieval.')
        results = list(hook.get_schema(table))
        output_array = []
        for i in results:
            new_dict = {}
            new_dict['name'] = i['COLUMN_NAME']
            new_dict['type'] = i['COLUMN_TYPE']

            if len(new_dict) == 2:
                output_array.append(new_dict)
        self.s3_upload(json.dumps(output_array), schema=True)

    def get_records(self, hook):
        logging.info('Initiating record retrieval.')
        logging.info('Start Date: {0}'.format(self.start))
        logging.info('End Date: {0}'.format(self.end))

        query = self.mysql_query
        params = self.mysql_params

        # Perform query and convert returned tuple to list
        results = hook.get_pandas_df(query, params)
        logging.info('Successfully performed query.')

        # Iterate through list of dictionaries (one dict per row queried)
        # and convert datetime and date values to isoformat.
        # (e.g. datetime(2017, 08, 01) --> "2017-08-01 00:00:00")
        results = results.to_csv(header=False, index=False,
                                 float_format="%.0f",
                                 date_format='%Y-%m-%d %H:%M:%S')
        self.s3_upload(results)
        return results

    def s3_upload(self, results, schema=False):
        s3 = S3Hook(aws_conn_id=self.s3_conn_id)
        key = '{0}'.format(self.s3_key)
        # If the file being uploaded to s3 is a schema, append "_schema" to the
        # end of the file name.
        if schema and key[-5:] == '.json':
            key = key[:-5] + '_schema' + key[-5:]
        if schema and key[-4:] == '.csv':
            key = key[:-4] + '_schema' + key[-4:]
        s3.load_string(
            string_data=results,
            bucket_name=self.s3_bucket,
            key=key,
            replace=True
        )
        logging.info('File uploaded to s3')
