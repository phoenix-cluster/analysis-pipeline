
import phoenixdb
import sys

"""
Get connection 
"""
def get_conn(host):
    database_url = 'http://' + host + ':8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)
    return conn





#testafdafda
host = "localhost"

conn = get_conn(host)
cursor = conn.cursor()
select_sql = "select SPEC_TITLE from V_PXD000222_SPEC_CLUSTER_MATCH  limit 1000"
cursor.execute(select_sql)
rs = cursor.fetchall()
if rs == None:
    sys.exit(0)
print(rs)


scored_psm_table = "T_PXD000021_SCORED_PSM_20171211"
recomm_new_table = "T_PXD000021_RECOMM_ID_20171211"

sql_str = "SELECT ID FROM " + scored_psm_table+ " WHERE ACCEPTANCE IS NULL"
cursor.execute(sql_str)
id_list = cursor.fetchall()

for id in id_list:
    break
    sql_str = "UPSERT INTO " + scored_psm_table + "(ID, ACCEPTANCE) VALUES (" + str(id[0]) + ",0)"
    cursor.execute(sql_str)


sql_str = "SELECT ID FROM " + recomm_new_table + " WHERE ACCEPTANCE IS NULL"
cursor.execute(sql_str)
id_list = cursor.fetchall()

for id in id_list:
    sql_str = "UPSERT INTO " + recomm_new_table + "(ID, ACCEPTANCE) VALUES (" + str(id[0]) + ",0)"
    cursor.execute(sql_str)




cursor.close()
conn.close()
