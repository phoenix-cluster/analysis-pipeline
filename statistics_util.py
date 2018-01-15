import phoenixdb

# import time
# import math
import os, sys
# import re, json


file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import confident_score_calc as cf_calc
import phoenix_import_util as phoenix

cluster_table_prefix = "V_CLUSTER"
lib_spec_table_prefix = cluster_table_prefix + "_SPEC"

"""
set thresholds for each project, by recreating the view
"""

default_threshods = {
    "cluster_size_threshold": 10,
    "cluster_ratio_threshold": 0.5,
    "conf_sc_threshold": 0.1,
    "spectrast_fval_threshold": 0.5
}


def set_threshold(project_id, thresholds, date, host):
    conn = phoenix.get_conn(host)
    cursor = conn.cursor()
    new_psm_view_name = "V_" + project_id.upper() + "_" + "NEW_PSM";
    pos_sc_psms_view_name = "V_" + project_id.upper() + "_" + "P_SCORE_PSM";
    neg_sc_psms_view_name = "V_" + project_id.upper() + "_" + "N_SCORE_PSM";
    better_psms_view_name = "V_" + project_id.upper() + "_" + "BETTER_PSM";
    matched_spec_view_name = "V_" + project_id.upper() + "_SPEC_CLUSTER_MATCH"

    drop_view_sql = "drop view if exists " + new_psm_view_name;
    cursor.execute(drop_view_sql)
    create_view_sql = "create view %s as select * from T_%s_NEW_PSM_%s where CLUSTER_RATIO >=%d and CLUSTER_SIZE >=%d and F_VAL >=%f and RECOMM_SEQ_SC >%f" % (
        new_psm_view_name, project_id.upper(), date,
        thresholds.get('cluster_ratio_threshold'),
        thresholds.get('cluster_size_threshold'),
        thresholds.get('spectrast_fval_threshold'),
        thresholds.get('conf_sc_threshold'),
    )
    cursor.execute(create_view_sql)

    drop_view_sql = "drop view if exists " + pos_sc_psms_view_name;
    cursor.execute(drop_view_sql)
    create_view_sql = "create view %s as select * from T_%s_SCORE_PSM_%s where CLUSTER_RATIO >=%d and CLUSTER_SIZE >=%d and F_VAL >=%f and CONF_SC >%f" % (
        pos_sc_psms_view_name, project_id.upper(), date,
        thresholds.get('cluster_ratio_threshold'),
        thresholds.get('cluster_size_threshold'),
        thresholds.get('spectrast_fval_threshold'),
        thresholds.get('conf_sc_threshold'),
    )
    cursor.execute(create_view_sql)

    drop_view_sql = "drop view if exists " + neg_sc_psms_view_name
    cursor.execute(drop_view_sql)
    create_view_sql = "create view %s as select * from T_%s_SCORE_PSM_%s where CLUSTER_RATIO >=%d and CLUSTER_SIZE >=%d and F_VAL >=%f and CONF_SC <0" % (
        neg_sc_psms_view_name, project_id.upper(), date,
        thresholds.get('cluster_ratio_threshold'),
        thresholds.get('cluster_size_threshold'),
        thresholds.get('spectrast_fval_threshold'),
    )
    cursor.execute(create_view_sql)

    drop_view_sql = "drop view if exists " + better_psms_view_name
    cursor.execute(drop_view_sql)
    create_view_sql = "create view %s as select * from T_%s_SCORE_PSM_%s where CLUSTER_RATIO >=%d and CLUSTER_SIZE >=%d and F_VAL >=%f and RECOMM_SEQ_SC >%f and CONF_SC <0" % (
        better_psms_view_name, project_id.upper(), date,
        thresholds.get('cluster_ratio_threshold'),
        thresholds.get('cluster_size_threshold'),
        thresholds.get('spectrast_fval_threshold'),
        thresholds.get('conf_sc_threshold'),
    )
    cursor.execute(create_view_sql)

    drop_view_sql = "drop view if exists " + matched_spec_view_name
    cursor.execute(drop_view_sql)
    create_view_sql = "create view %s as select * from T_%s_SPEC_CLUSTER_MATCH_%s where CLUSTER_RATIO >=%d and CLUSTER_SIZE >=%d and F_VAL >=%f " % (
        matched_spec_view_name, project_id.upper(), date,
        thresholds.get('cluster_ratio_threshold'),
        thresholds.get('cluster_size_threshold'),
        thresholds.get('spectrast_fval_threshold'),
    )
    cursor.execute(create_view_sql)

    cursor.close()
    conn.close()


def get_row_count(table_name, cursor):
    select_sql = "select count(*) from %s" % (table_name)
    cursor.execute(select_sql)
    rs = cursor.fetchone()
    if rs == None:
        return None
    return rs[0]

def get_sum_spec(table_name, cursor):
    select_sql = "select sum(NUM_SPEC) from %s" % (table_name)
    cursor.execute(select_sql)
    rs = cursor.fetchone()
    if rs == None:
        return None
    return rs[0]



def get_matched_id_spec_no(project_id, cursor):
    select_sql = "select count(*) from V_%s_SPEC_CLUSTER_MATCH a, T_%s_PSM b where a.SPEC_TITLE = b.SPECTRUM_TITLE"  % (project_id.upper(), project_id.upper())
    cursor.execute(select_sql)
    rs = cursor.fetchone()
    if rs == None:
        return None
    return rs[0]


def get_statistics_data(project_id, host):
    """

    :param project_id:
    :param host:
    :return:


                       "prePSM_no, INTEGER" + \
                       "prePSM_not_mathced_no, INTEGER, " + \
                       "prePSM_high_conf_no, INTEGER, " + \
                       "prePSM_low_conf_no, INTEGER, " + \
                       "matched_spec_no, INTEGER, " + \
                       "better_PSM_no, INTEGER, " + \
                       "new_PSM_no, INTEGER, " + \

    """
    conn = phoenix.get_conn(host)
    cursor = conn.cursor()

    statistics_results = dict()
    new_psm_view_name = "V_" + project_id.upper() + "_" + "NEW_PSM"
    pos_sc_psms_view_name = "V_" + project_id.upper() + "_" + "P_SCORE_PSM"
    neg_sc_psms_view_name = "V_" + project_id.upper() + "_" + "N_SCORE_PSM"
    better_psms_view_name = "V_" + project_id.upper() + "_" + "Better_PSM"
    matched_table_name = "V_" + project_id.upper() + "_SPEC_CLUSTER_MATCH"
    ident_table_name = "T_" + project_id.upper() + "_PSM"

    matched_id_spec_no = get_matched_id_spec_no(project_id, cursor)
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
    return statistics_results
