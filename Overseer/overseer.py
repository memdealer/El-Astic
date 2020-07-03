import configparser
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import Search

#Setting configparser object
config = configparser.ConfigParser(inline_comment_prefixes="#")
config.sections()
config.read('overseer.ini')
config.sections()

#Getting values out of configparser
search_after = config.get('Overseer', 'time_window')
jobs_oversee_list = config.get('Overseer', 'jobs_oversee_list')
ignore_ppc_sending_list = config.get('Overseer', 'ignore_ppc_sending')
ignore_job_list = config.get('Overseer', 'ignore_job')
search_index = config.get('ElasticSearch', 'search_index')
max_job_search = config.get('ElasticSearch', 'max_job_search')
elastic_server = config.get('ElasticSearch', 'elastic_server')
es_db_password = config.get('ElasticSearch', 'es_db_password')
es_db_user = config.get('ElasticSearch', 'es_db_user')

#Convert to list
ignore_job_list = ignore_job_list.split(",")
jobs_oversee_list = jobs_oversee_list.split(",")
ignore_ppc_sending_list = ignore_ppc_sending_list.split(",")

#Plain connection to ES cluster
elastic = Elasticsearch(
    [elastic_server],
    http_auth = (es_db_user, es_db_password),
    scheme = "https",
    port = 9200,
    verify_certs = False,
    ssl_show_warn = False

)


def ignore_this(search_line,ignore_list):
""" Returns False in case no matching has been found within given arguments and ignore list"""
    search_line = str(search_line).lower()
    for element in ignore_list:
        if element in search_line:
            return True
    return False


def find_corrupted_job(job_name):
""" Returns a list of hits """
    s = Search(index=search_index) \
    .using(elastic) \
    .filter("exists", field="cv_error_reason") \
    .filter('range', **{'@timestamp': {'gte': "now-{0}h".format(search_after), 'lt': 'now'}}) \
    .query("wildcard", cv_production_name=job_name) \
    .extra(size="{0}".format(max_job_search))

    list = []
    for hit in s:
        if ignore_this(hit.cv_error_reason,ignore_ppc_sending_list) == False:
            list.append(" JOB: " + hit.cv_production_name.upper() + " ERROR:=> " + hit.cv_error_reason + " END " + "\\n")
        if ignore_this(hit.cv_production_name,ignore_job_list) == False:
            list.append("JOB: " + hit.cv_production_name.upper() + " ERROR:=> " + hit.cv_error_reason + " END " + "\\n")
    line = [i.replace('\n', '') for i in list]
    return line

#Check_MK metrics
for job in jobs_oversee_list:
    resulting_list = find_corrupted_job(job)
    if len(resulting_list) >= 1:
        print("2 " + job + " - CRIT: " + str.strip(str(resulting_list), "[]''"))
    else:
        print("0 " + job + " - OK: No errors spotted")
