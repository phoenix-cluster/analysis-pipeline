import pymysql.cursors
import urllib3
import os,sys

"""
This program get the phospho projects which are also in 201504 PRIDE Cluster.
And also the identification number in these projects.
"""


mysql_host = "192.168.6.20"
get_id_peptides_url_pre = "http://www.ebi.ac.uk/pride/ws/archive/peptide/count/project/"
select_prjs = "SELECT project_id from 201504_3_projects"
http = urllib3.PoolManager()


def get_id_peptides_no(project_id):

    get_id_peptides_url = get_id_peptides_url_pre + project_id
    try:
        response = http.request('GET', get_id_peptides_url, timeout=1000.0, retries=1000)
        if response.status == 200:
            return int(response.data)
        else:
            print("Failed to get the number of identified peptides for project !", project_id)
            return -1
    except Exception as e :
        print(e)
        print("Failed to get the number of identified peptides for project !", project_id)
        return -2 




connection = pymysql.connect(host=mysql_host,
                                          user='pride_cluster_t',
                                          password='pride123',
                                          db='pride_cluster_t',
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
print("Opened database successfully");

projects_in_db = set() 
projects_in_pho = set() 
projects_intersection = set()
with connection.cursor() as cursor:
    cursor.execute(select_prjs)
    results = cursor.fetchall()
    connection.commit()
    for result in results:
        project_id = result.get("project_id")
        projects_in_db.add(project_id)
#print(projects_in_db)

with open("/home/ubuntu/mingze/tppdata/project_ids.txt") as f:
    for line in f:
        projects_in_pho.add(line.strip())

#print(projects_in_pho)

projects_intersection = projects_in_pho.intersection(projects_in_db) 

print("We got %d intersection projects:%s" % (len(projects_intersection), projects_intersection))

for project in projects_intersection:
    N_peptides_identified = get_id_peptides_no(project)
    print(project, end='\t\t')
    print(str(N_peptides_identified), end='\t\n')
