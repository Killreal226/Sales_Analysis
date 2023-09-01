import sqlite3
import pandas as pd 
from sqlalchemy import create_engine

#Подключение к базе данных
conn = sqlite3.connect('src/database.db')


#------Создание таблиц pandas из подключенной базы данных-------
query_task = 'SELECT * FROM task'
df_task = pd.read_sql_query(query_task, conn)

query_call = 'SELECT * FROM call'
df_call = pd.read_sql_query(query_call, conn)

query_action = 'SELECT * FROM action'
df_action = pd.read_sql_query(query_action, conn)

query_product = 'SELECT * FROM product'
df_product = pd.read_sql_query(query_product, conn)

query_emp_x_org_gr = 'SELECT * FROM emp_x_org_gr'
df_emp_x_org_gr = pd.read_sql_query(query_emp_x_org_gr, conn)

query_result = 'SELECT * FROM result'
df_result = pd.read_sql_query(query_result, conn)

query_queue = 'SELECT * FROM queue'
df_queue = pd.read_sql_query(query_queue, conn)

query_group = 'SELECT * FROM "group"'
df_group = pd.read_sql_query(query_group, conn)

query_mngmnt = 'SELECT * FROM mngmnt'
df_mngmnt = pd.read_sql_query(query_mngmnt, conn)

query_horoscope = 'SELECT * FROM horoscope'
df_horoscope = pd.read_sql_query(query_horoscope, conn)

#-----Объеденение таблиц call и action по столбцу "Ключ коммуникации с клиентом"---------
df_merged = df_call.merge(df_action, left_on='wo_hit_rk', right_on='hit_rk', how='inner')
df_merged.drop(['index_y','hit_rk'], axis=1, inplace=True)
df_merged.rename(columns={'wo_hit_rk': 'hit_rk'}, inplace=True)

#-----Объеденение общей таблицы и таблицы action по столбцу "Ключ коммуникации с клиентом"---------
df_merged = df_merged.merge(df_product, left_on='hit_rk', right_on='hit_rk', how='inner')
df_merged.drop('index', axis=1, inplace=True)

#-----Объеденение общей таблицы и таблицы task по столбцу "Ключ задания"---------
df_merged = df_merged.merge(df_task, left_on='wo_task_rk', right_on='task_rk', how='inner')
df_merged.drop(['index','task_rk'], axis=1, inplace=True)
df_merged.rename(columns={'wo_task_rk': 'task_rk'}, inplace=True)

#-----Объеденение общей таблицы и таблицы emp_x_org_gr по столбцу "Ключ оператора"---------
df_merged = df_merged.merge(df_emp_x_org_gr, left_on='wo_employee_rk', right_on='employee_rk', how='inner')
df_merged.drop(['index','employee_rk'], axis=1, inplace=True)
df_merged.rename(columns={'wo_employee_rk': 'employee_rk'}, inplace=True)

#-----Объеденение общей таблицы и таблицы result по столбцу "Идентификтор результата коммуникации"---------
df_merged = df_merged.merge(df_result, left_on='hit_status_result_id', right_on='hit_status_result_id', how='inner')
df_merged.drop(['index'], axis=1, inplace=True)

#-----Объеденение общей таблицы и таблицы queue по столбцу "Идентификтор очереди, на которой получено задание"---------
df_merged = df_merged.merge(df_queue, left_on='wo_queue_id', right_on='queue_id', how='inner')
df_merged.drop(['index','queue_id'], axis=1, inplace=True)
df_merged.rename(columns={'wo_queue_id': 'queue_id'}, inplace=True)

#-----Объеденение общей таблицы и таблицы group по столбцу "Ключ группы, в которой работает оператор"---------
df_merged = df_merged.merge(df_group, left_on='org_group_rk', right_on='org_group_rk', how='inner')
df_merged.drop(['index'], axis=1, inplace=True)

#-----Объеденение общей таблицы и таблицы group по столбцу "Ключ управления, в которой работает оператор"---------
df_merged = df_merged.merge(df_mngmnt, left_on='org_management_rk', right_on='org_management_rk', how='inner')
df_merged.drop(['index'], axis=1, inplace=True)

#-----Объеденение общей таблицы и таблицы group по столбцу "Логин оператора"---------
df_merged = df_merged.merge(df_horoscope, left_on='agent_login', right_on='agent_login', how='inner')
df_merged.drop(['index','index_x'], axis=1, inplace=True)

#------Пеерименование столбцов времени (одинаково назывались)------
df_merged.rename(columns={'finish_dttm_x': 'finish_dttm_call'}, inplace=True)
df_merged.rename(columns={'finish_dttm_y': 'finish_dttm_task'}, inplace=True)

#-----------Сортировка и перестановка столбцов, чтобы данные выглядели понятно---------
df_merged = df_merged.sort_values(['management_nm','group_nm','agent_login','hit_status_result_id','hid'])
df_merged = df_merged.reindex(columns=[
    'management_nm',
    'org_management_rk',
    'group_nm',
    'org_group_rk',
    'agent_login',
    'employee_rk',
    'horoscope',
    'task_rk',
    'task_stage_id',
    'source_system_cd',
    'create_dttm',
    'finish_dttm_task',
    'hit_rk',
    'queue_desc',
    'queue_id',
    'finish_dttm_call',
    'duratoin_sec',
    'hit_status_result_desc',
    'hit_status_result_id',
    'hid',
    'using_flg',
    ])

#-----Создание новой базы данных с одной общей таблицей-------
engine = create_engine('sqlite:///joined_database.db')
with engine.begin() as connection:
    df_merged.to_sql('merged', con=connection)

#-----Создание общей таблице в Exel----------
df_merged.to_excel('joined_table.xlsx', index=False)

#------Отключение от базы данных-------
conn.close()

print('Таблица успешно создана и записана')

