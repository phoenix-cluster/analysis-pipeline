
import os, sys
import logging


file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
# import phoenix_storage_access as phoenix
import mysql_storage_access as mysql_acc

cluster_table_prefix = "V_CLUSTER"
lib_spec_table_prefix = cluster_table_prefix + "_SPEC"

"""
set thresholds for each project, by recreating the view
"""

default_thresholds = {
    "cluster_size_threshold": 10,
    "cluster_ratio_threshold": 0.5,
    "conf_sc_threshold": 0.1,
    "spectrast_fval_threshold": 0.5,
    "min_seq_no_in_species_threshold":10
}





def create_views(project_id, thresholds):
    """"""
    conn = mysql_acc.get_conn()
    cursor = conn.cursor()
    mysql_acc.create_project_ana_record_table()
    new_psm_view_name = "V_" + project_id.upper() + "_" + "NEW_PSM";
    pos_sc_psms_view_name = "V_" + project_id.upper() + "_" + "P_SCORE_PSM";
    neg_sc_psms_view_name = "V_" + project_id.upper() + "_" + "N_SCORE_PSM";
    better_psms_view_name = "V_" + project_id.upper() + "_" + "BETTER_PSM";
    matched_spec_view_name = "V_" + project_id.upper() + "_SPEC_CLUSTER_MATCH"

    drop_view_sql = "drop view if exists " + new_psm_view_name;
    cursor.execute(drop_view_sql)
    create_view_sql = "create view %s as select * from T_%s_NEW_PSM " % (
        new_psm_view_name, project_id.upper())
    try:
        cursor.execute(create_view_sql)
        logging.info("Done with sql: %s"%(create_view_sql))
    except :
        print("error in exceute SQL: %s" % (create_view_sql))


    drop_view_sql = "drop view if exists " + pos_sc_psms_view_name;
    cursor.execute(drop_view_sql)
    create_view_sql = "create view %s as select * from T_%s_P_SCORE_PSM " % (
        pos_sc_psms_view_name, project_id.upper())
    try:
        cursor.execute(create_view_sql)
        logging.info("Done with sql: %s"%(create_view_sql))
    except :
        print("error in exceute SQL: %s" % (create_view_sql))

    drop_view_sql = "drop view if exists " + neg_sc_psms_view_name
    cursor.execute(drop_view_sql)
    create_view_sql = "create view %s as select * from T_%s_N_SCORE_PSM" % (
        neg_sc_psms_view_name, project_id.upper())
    try:
        cursor.execute(create_view_sql)
        logging.info("Done with sql: %s"%(create_view_sql))
    except :
        print("error in exceute SQL: %s" % (create_view_sql))

    drop_view_sql = "drop view if exists " + better_psms_view_name
    cursor.execute(drop_view_sql)
    create_view_sql = "create view %s as select * from T_%s_N_SCORE_PSM where recomm_seq_sc >= 0" % (
        better_psms_view_name, project_id.upper())
    try:
        cursor.execute(create_view_sql)
        logging.info("Done with sql: %s"%(create_view_sql))
    except :
        print("error in exceute SQL: %s" % (create_view_sql))

    drop_view_sql = "drop view if exists " + matched_spec_view_name
    cursor.execute(drop_view_sql)
    create_view_sql = "create view %s as select * from T_%s_SPEC_CLUSTER_MATCH where CLUSTER_RATIO >=%d and CLUSTER_SIZE >=%d and F_VAL >=%f " % (
        matched_spec_view_name, project_id.upper(),
        thresholds.get('cluster_ratio_threshold'),
        thresholds.get('cluster_size_threshold'),
        thresholds.get('spectrast_fval_threshold'),
    )
    try:
        cursor.execute(create_view_sql)
        logging.info("Done with sql: %s"%(create_view_sql))
    except :
        print("error in exceute SQL: %s" % (create_view_sql))

    #persist the thresholds to  db
#    phoenix.insert_thresholds_to_record(cursor, project_id, thresholds)
    mysql_acc.insert_thresholds_to_record(project_id, thresholds)
    conn.commit()
    cursor.close()
    conn.close()


def get_row_count(table_name, cursor):
    select_sql = "select count(*) from %s" % (table_name)
    try:
        cursor.execute(select_sql)
        rs = cursor.fetchone()
        if rs == None:
            return 0
        return rs[0]
    except Exception as e:
        logging.error("ERROR in executing sql: %s"%select_sql)
        logging.error(e)
        return 0

def get_sum_spec(table_name, cursor):
    select_sql = "select sum(NUM_SPEC) from %s" % (table_name)
    try:
        cursor.execute(select_sql)
        rs = cursor.fetchone()
        if rs == None:
            return 0
        return rs[0]
    except Exception as e:
        logging.error("ERROR in executing sql: %s, info: %s"%(select_sql, e))
        return 0



def get_matched_id_spec_no(project_id, identified_spectra, cursor):
    if identified_spectra is None or len(identified_spectra) < 1:
        return 0

    matched_spec = set()
    try:
        select_sql = "select spec_title from V_%s_SPEC_CLUSTER_MATCH "%project_id.upper()
        cursor.execute(select_sql)
        rs = cursor.fetchall()
        for r in rs:
            matched_spec.add(r[0])

        identified_spec = set(identified_spectra.keys())
        intersection_spec = matched_spec.intersection(identified_spec)
        print("get %d intersection spec"%len(intersection_spec))
        return (len(intersection_spec))
    except Exception as e:
        logging.ERROR("ERROR in executing sql: %s, info: %s"%(select_sql, e))
        return 0


    # select_sql = "select count(*) from V_%s_SPEC_CLUSTER_MATCH a, T_%s_PSM b where a.SPEC_TITLE = b.SPECTRUM_TITLE"  % (project_id.upper(), project_id.upper())
    # cursor.execute(select_sql)
    # rs = cursor.fetchone()
    # if rs == None:
    #     return 0
    # return rs[0]


def calc_and_persist_statistics_data(project_id, identified_spectra):
    """

    :param project_id:
    :return:


                       "prePSM_no, INTEGER" + \
                       "prePSM_not_mathced_no, INTEGER, " + \
                       "prePSM_high_conf_no, INTEGER, " + \
                       "prePSM_low_conf_no, INTEGER, " + \
                       "matched_spec_no, INTEGER, " + \
                       "better_PSM_no, INTEGER, " + \
                       "new_PSM_no, INTEGER, " + \

    """
#    conn = phoenix.get_conn(host)
    conn = mysql_acc.get_conn()
    cursor = conn.cursor()

    statistics_results = dict()
    # new_psm_view_name = "V_" + project_id.upper() + "_" + "NEW_PSM"
    # pos_sc_psms_view_name = "V_" + project_id.upper() + "_" + "P_SCORE_PSM"
    # neg_sc_psms_view_name = "V_" + project_id.upper() + "_" + "N_SCORE_PSM"
    # better_psms_view_name = "V_" + project_id.upper() + "_" + "Better_PSM"
    # matched_table_name = "V_" + project_id.upper() + "_SPEC_CLUSTER_MATCH"
    new_psm_view_name = "V_" + project_id.upper() + "_" + "NEW_PSM"
    pos_sc_psms_view_name = "V_" + project_id.upper() + "_" + "P_SCORE_PSM"
    neg_sc_psms_view_name = "V_" + project_id.upper() + "_" + "N_SCORE_PSM"
    better_psms_view_name = "V_" + project_id.upper() + "_" + "BETTER_PSM"
    matched_table_name = "V_" + project_id.upper() + "_SPEC_CLUSTER_MATCH"
    ident_table_name = "T_" + project_id.upper() + "_PSM"

    matched_id_spec_no = get_matched_id_spec_no(project_id, identified_spectra, cursor)
    prePSM_no = get_row_count(ident_table_name, cursor)

    statistics_results['prePSM_no'] = prePSM_no
    statistics_results['prePSM_not_matched_no'] = prePSM_no - matched_id_spec_no
    statistics_results['prePSM_high_conf_no'] = get_sum_spec(pos_sc_psms_view_name, cursor)
    statistics_results['prePSM_low_conf_no'] = get_sum_spec(neg_sc_psms_view_name, cursor)
    statistics_results['better_PSM_no'] = get_sum_spec(better_psms_view_name, cursor)
    statistics_results['new_PSM_no'] = get_sum_spec(new_psm_view_name, cursor)
    statistics_results['matched_spec_no'] = get_row_count(matched_table_name, cursor)
    statistics_results['matched_id_spec_no'] = matched_id_spec_no
    cursor.close()
    conn.close()

    for key in statistics_results.keys():
        if (statistics_results.get(key) == None):
            statistics_results[key] = 0

    logging.info(statistics_results)
#    phoenix.insert_statistics_to_record(project_id, statistics_results)
    mysql_acc.insert_statistics_to_record(project_id, statistics_results)
    return statistics_results

