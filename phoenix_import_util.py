import phoenixdb
import time
import math
import sys,re,json

cluster_table = "cluster_table_test_24102017"
lib_spec_table = cluster_table + "_spec"


"""
Get sequences' ratio from cluster, when the sequence is not matched to the max_seq
If no seq matched, return 0.0
"""
def get_seq_ratio(spectrum_pep, cluster_id, conn):
    spectrum_pep = spectrum_pep.replace("I", "L")
    spectrum_pep_list = spectrum_pep.split("||")

    sql_str = "SELECT SPEC_TITLE, ID_SEQUENCES, SEQ_RATIO FROM " + lib_spec_table + \
        "WHERE CLUSTER_FK = " + cluster_id
    this_seq_ratio = 0.0
    with conn.cursor() as cursor :
        cursor.execute(sql_str)
        rs = cursor.fetchall
        for r in rs:
            spec_title = r[0]
            id_seqs = r[1]
            seq_ratio = r[2]
            id_seq_list = id_seqs.split("||")
            for id_seq in id_seq_list:
                for spec_pep_seq in spectrum_pep_list:
                    if id_seq == spec_pep_seq:
                        this_seq_ratio = seq_ratio
                        return this_seq_ratio
    return this_seq_ratio

"""
Calculate the confident scores for Original Pep-Spec-Match
Based on our scoring model
"""
def calculate_conf_sc(prj_id, search_results, conn):
    clusters = get_lib_rs_from_phoenix(search_results, conn)
    conf_scs = {}
    print("Calculating confident scores")
    spectra_pep =get_spectra_pep(prj_id, conn)
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        lib_spec_id = search_result.get('lib_spec_id')
        dot = search_result.get('dot')
        fval = search_result.get('fval')

        cluster = clusters.get(lib_spec_id)
        #todo comment it. this is only for debug
        if cluster == None:
            print("Warnning! Got null matched cluster for " + lib_spec_id)
            continue
        ratio = cluster.get('ratio')
        n_spec = cluster.get('n_spec') 
        seqs_ratios_str = cluster.get('seqs_ratios')

        if n_spec > 1000:    #we assume n>=1000. the contribution of n_spec is al the same
            cutted_n_spec = 1000
        else :
            cutted_n_spec = n_spec

        spec_pep_str = spectra_pep.get(spec_title)
        pep_seqs = list()
        if spec_pep_str == None or spec_pep_str == "":
            pep_seqs.append("RECOMMEND")
#            raise  Exception("Got None peptide sequence for %s" % spec_title)
        else:
            spec_pep_str = spec_pep_str.replace("I","L") #replace acid I to L
            spec_pep_str = spec_pep_str.replace("||", ",")
            spec_pep_str = spec_pep_str.replace("|", ",")
            spec_pep_str = spec_pep_str.rstrip()
            pep_seqs = spec_pep_str.split(",")
        seq_scores = list()
        max_score = -1000000.0
        max_score_seq = ""
        for pep_seq in pep_seqs:
            (seq_score,returned_pep_seq) = calculate_conf_sc_for_a_seq(pep_seq, n_spec, seqs_ratios_str, ratio, lib_spec_id)#the returned pep_seq could be recommend sequence for the non identified spectrum
            if seq_score > max_score:
                max_score = seq_score
                max_score_seq = returned_pep_seq
            seq_scores.append(seq_score)
        if len(pep_seqs)>1:
            print("This spectrum has multiple PSMs, we chose the max score %f for %s"%(max_score, max_score_seq))
        conf_scs[spec_title] = {"conf_score":max_score, "recommend_pep_seq":max_score_seq}
    return conf_scs

def calculate_conf_sc_for_a_seq(pep_seq, n_spec, seqs_ratios_str, ratio, lib_spec_id):
        normalized_n_spec = math.log(n_spec)/math.log(1000)
        no_pre_identification = False
        # allUpper = re.compile(r'^[A-Z]')
        # if allUpper.match(spectrum_pep):
        allUpper = re.compile(r'[^A-Z]')
        if allUpper.match(pep_seq):
            print(allUpper.match(pep_seq))
            raise Exception("Peptide sequence is not all upper case letter: " + pep_seq)\

        #transfer the string to standard json string
        seqs_ratios_str = seqs_ratios_str.replace("'","\"")
        seqs_ratios_str = seqs_ratios_str.replace(": ",": \"")
        seqs_ratios_str = seqs_ratios_str.replace(",","\",")
        seqs_ratios_str = seqs_ratios_str.replace("}","\"}")
        seqs_ratios = json.loads(seqs_ratios_str)


        this_seq_ratio = 0.0
        max_seq_ratio = 0.0
        max_seq = ""
        other_ratios = dict()
        for seq in seqs_ratios.keys():
            other_ratios[seq] = float(seqs_ratios.get(seq))
            if other_ratios[seq] > max_seq_ratio:
                max_seq_ratio = float(other_ratios[seq])
                max_seq = seq

        if max_seq_ratio > ratio + 0.01 or max_seq_ratio < ratio - 0.01:
            raise Exception("The max-seq_ratio is not equal to the ratio in database with cluster %s : %s, %f"%(lib_spec_id, seqs_ratios_str, ratio))

        try:
            if pep_seq == "RECOMMEND":
                pep_seq = max_seq
                no_pre_identification = True
            this_seq_ratio = seqs_ratios.get(pep_seq)
            other_ratios.pop(pep_seq)
        except KeyError as ex:
            print("No such key: '%s'" % ex)
            #assign a new ratio for this seq, and adjust the others
            if this_seq_ratio ==None or this_seq_ratio == "":
                this_seq_ratio = 1.0/(n_spec + 1)
                adjust_factor = n_spec/(n_spec + 1.0)
                for akey in other_ratios.keys():
                    other_ratios[akey] *= adjust_factor
            #raise Exception("Got None ratio for this peptide sequence %s from seqs_ratios %s in cluster %s" % (pep_seq, seqs_ratios_str, lib_spec_id))

        this_seq_ratio = float(this_seq_ratio)

        #some time, multiple sequences could be assigned to one spectrum, cause a sum of ratios more than 1.
        #for this situation, we pick the other ratios from small to big, to make a set of ratios to "1".
        #here we modified the other_ratios list
        sum_ratio = 0.0
        for temp_value in seqs_ratios.values():
            sum_ratio += float(temp_value)
        if sum_ratio > 1.001:
            new_other_ratios = dict()
            max_sum_others = 1 - this_seq_ratio
            i = 0
            print("sum of all ratios: " + str(sum_ratio))
            print(other_ratios)
            if max_sum_others > 0:
                for temp_ratio in sorted(other_ratios.values()):
                    if sum(new_other_ratios.values()) < max_sum_others:
                        new_other_ratios[str(i)] = temp_ratio
                        print("add a new ratio " + str(temp_ratio))
                        i += 1
                offset = sum(new_other_ratios.values()) - max_sum_others
                print("offset is" + str(offset))
                new_other_ratios[str(len(new_other_ratios)-1)] -= offset
                other_ratios = new_other_ratios

        sum_sqr_of_others = 0.0
        for other_ratio in other_ratios.values():
            sum_sqr_of_others += pow(other_ratio,2)
        sqrt_of_others = math.sqrt(sum_sqr_of_others)
        confident_score = normalized_n_spec * (this_seq_ratio - sqrt_of_others)
#        print("conf_sc %f for pep seq %s in cluster %s " % (confident_score, pep_seq, lib_spec_id))
#        print("normalized_n_spec %f * (this_seq_ratio %f - sqrt_of_others %f)" % (normalized_n_spec , this_seq_ratio , sqrt_of_others))
        if this_seq_ratio ==  0.5 and confident_score == 0:  #
            confident_score = - 0.1  #penalizing score -0.1 for (0.5 0.5)

        if no_pre_identification:
            pep_seq = "R_NEW_" + pep_seq
        else:
            if confident_score < 0 and max_seq_ratio > 0.5:
                pep_seq = "R_Better_" + max_seq
            else:
                pep_seq = "PRE_" + pep_seq
        return (confident_score, pep_seq)


"""
Get spectra identify data from identification table
"""       
def get_spectra_pep(prj_id, conn):
    id_table = "IDENTIFICATIONS_" + prj_id.upper() + "_TEST17102017"
    sql_str = "SELECT SPEC_TITLE, PEP_SEQ FROM " + id_table + ""
    print(sql_str)
    cursor = conn.cursor()
    cursor.execute(sql_str)
    spectra_rs = cursor.fetchall()
    spectra_pep = {}
    for r in spectra_rs:
        spectra_title = r[0]
        pep_seq = r[1]
        spectra_pep[spectra_title] = pep_seq
    return spectra_pep


"""
Export search result of a project to phoenix/hbase table
"""
def export_sr_to_phoenix(project_id, host, search_results):
    """    
    "spec_title varchar(100) NOT NULL,"    + \
    "scan_in_lib int(15) COLLATE utf8_bin NOT NULL,"    + \
    "dot float NOT NULL,"    + \
    "delta float NOT NULL,"    + \
    "dot_bias float NOT NULL,"    + \
    "mz_diff float NOT NULL,"    + \
    "fval float NOT NULL,"    + \
    """
    table_name = "lib_search_sr_" + project_id  + "test_" + time.strftime("%d%m%Y")
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + table_name.upper() + "\" (" + \
             "spec_title VARCHAR NOT NULL PRIMARY KEY ," + \
             "spec_in_lib VARCHAR ,"   + \
             "dot FLOAT ,"   + \
             "f_val FLOAT, "  + \
             "conf_sc FLOAT, "  + \
             "recommend_pep VARCHAR "  + \
             ")"

    database_url = 'http://' + host + ':8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)
    cursor = conn.cursor()
    cursor.execute(create_table_sql)

    conf_sc_set = calculate_conf_sc(project_id, search_results, conn)
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        spec_in_lib = search_result.get('lib_spec_id')
        dot = search_result.get('dot')
        fval = search_result.get('fval')
        conf_sc = conf_sc_set[spec_title]['conf_score']
        recommend_pep_seq = conf_sc_set[spec_title]['recommend_pep_seq']

        upsert_sql = "UPSERT INTO " + table_name + " VALUES (?, ?, ?, ?, ?, ?)"
        cursor.execute(upsert_sql, (spec_title, spec_in_lib, dot, fval, conf_sc, recommend_pep_seq))

    cursor.close()
    conn.close()

"""
Get all matched cluster results from phoenix 
"""
def get_lib_rs_from_phoenix(search_results, conn):
    lib_result = dict()
    
    cursor = conn.cursor()
    clusters = dict()
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        spec_in_lib = search_result.get('lib_spec_id')
        sql_str = "SELECT CLUSTER_RATIO, N_SPEC, N_ID_SPEC, N_UNID_SPEC, SEQUENCES_RATIOS from \"" + cluster_table +"\" WHERE CLUSTER_ID = '" + spec_in_lib + "'"
        cursor.execute(sql_str)
        rs = cursor.fetchone()
        if rs == None:
            continue
        cluster = {}
        cluster['ratio'] = rs[0]
        cluster['n_spec'] = rs[1]
        cluster['seqs_ratios'] = rs[4]
        clusters[spec_in_lib] = cluster
    cursor.close()
    if len(clusters) < 1:
        raise Exception("Got empty cluster set for this search result")
    return(clusters)
        
"""
Export identification result of a project to phoenix/hbase table
"""
def export_ident_to_phoenix(project_id, host, identifications):
    """
        o.write("spectrum_title\tsequence\n")
        for spec_title in identifications.keys():
            o.write("%s\t%s\n"%(spec_title, identifications[spec_title]))
    """

    start = time.time()
    print("start phoenix inserting at " + str(start))
    table_name = "identifications_" + project_id + "_" + time.strftime("%d%m%Y")
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + table_name.upper() + "\" (" + \
             "spec_title VARCHAR NOT NULL PRIMARY KEY , " + \
             "pep_seq VARCHAR "   + \
             ")"
    database_url = 'http://' + host + ':8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)
    cursor = conn.cursor()

    cursor.execute(create_table_sql)

    for spec_title in identifications.keys():
        upsert_sql = "UPSERT INTO " + table_name + " VALUES (?, ?)"
        cursor.execute(upsert_sql, (spec_title,identifications[spec_title]))

    cursor.execute("SELECT count(*) FROM " + table_name)
    print(cursor.fetchone())

    cursor.close()
    conn.close()

    end = time.time()
    print("end phoenix inserting at " + str(end))
    print("totally time " + str(end - start))


def import_clusters_to_phoenix(connection):
    old_cluster_table = "201504_3"
#    ratio_threshold = "0.618"
    size_threshold = 2
    clusters = dict()

    select_str = "SELECT cluster_id,cluster_ratio, n_spec FROM %s WHERE n_spec>=%d"% \
                 (old_cluster_table, size_threshold)
    with connection.cursor() as cursor:
        cursor.execute(select_str)
    results = cursor.fetchall()

    start = time.time()
    print("start phoenix inserting at " + str(start))
    table_name = "identifications_" + project_id + "_" + time.strftime("%d%m%Y")
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + table_name.upper() + "\" (" + \
             "spec_title VARCHAR NOT NULL PRIMARY KEY , " + \
             "pep_seq VARCHAR "   + \
             ")"
    database_url = 'http://' + host + ':8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)
    cursor = conn.cursor()

    cursor.execute(create_table_sql)

    """
    with open(cluster_list_file, 'w') as o:
        o.write("%s\t%s\t%s\n"%('cluster_id','n_spec', 'ratio'))
        for result in results:
            cluster_id = result.get('cluster_id')
            n_spec = result.get('n_spec')
            ratio = result.get('cluster_ratio')
            o.write("%s\t%s\t%s\n"%(cluster_id, n_spec, ratio))
    """

    cursor.execute("SELECT count(*) FROM " + table_name)
    print(cursor.fetchone())

    cursor.close()
    conn.close()

    end = time.time()
    print("end phoenix inserting at " + str(end))
    print("totally time " + str(end - start))





project_id = 'test00002'
host = 'localhost'
search_results = {} 
identifications = {} 

result1 = {'lib_spec_id':"000001f5-cb21-4db9-9ece-6ea8ab3a7ded", 'dot':"0.1", 'fval':"0.1"}
result2 = {'lib_spec_id':"0000047f-5bdf-4302-9c1f-8fe7fa0f2971", 'dot':"0.1", 'fval':"0.1"}
result3 = {'lib_spec_id':"00000839-e89d-4e99-ac51-1f370173bf8f", 'dot':"0.1", 'fval':"0.1"}
search_results['title1'] = result1
search_results['title2'] = result1
search_results['title3'] = result1
identifications['title1'] = "squence1"
identifications['title2'] = "squence2"
identifications['title3'] = "squence3"



database_url = 'http://localhost:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)
#get_lib_rs_from_phoenix(search_results, conn)
#get_spectra("PXD000021", conn)
"""
export_sr_to_phoenix(project_id, host, search_results)
export_ident_to_phoenix(project_id, host, identifications)
cursor = conn.cursor()
cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
cursor.execute("UPSERT INTO users VALUES (?, ?)", (1, 'admin'))
cursor.execute("UPSERT INTO users VALUES (?, ?)", (2, 'user1'))
cursor.execute("UPSERT INTO users VALUES (?, ?)", (3, 'user2'))
cursor.execute("SELECT * FROM users")
print(cursor.fetchall())
"""



