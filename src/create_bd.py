import pandas as pd
from sqlalchemy import create_engine

df_task = pd.read_excel('Витрины.xlsx', sheet_name='task')
df_call = pd.read_excel('Витрины.xlsx', sheet_name='call')
df_action = pd.read_excel('Витрины.xlsx', sheet_name='action')
df_product = pd.read_excel('Витрины.xlsx', sheet_name='product')
df_emp_x_org_gr = pd.read_excel('Витрины.xlsx', sheet_name='emp_x_org_gr')
df_result = pd.read_excel('Витрины.xlsx', sheet_name='result')
df_queue = pd.read_excel('Витрины.xlsx', sheet_name='queue')
df_group = pd.read_excel('Витрины.xlsx', sheet_name='group')
df_mngmnt = pd.read_excel('Витрины.xlsx', sheet_name='mngmnt')
df_horoscope = pd.read_excel('Витрины.xlsx', sheet_name='horoscope')

engine = create_engine('sqlite:///database.db')
with engine.begin() as connection:
    df_task.to_sql('task', con=connection)
    df_call.to_sql('call', con=connection)
    df_action.to_sql('action', con=connection)
    df_product.to_sql('product', con=connection)
    df_emp_x_org_gr.to_sql('emp_x_org_gr', con=connection)
    df_result.to_sql('result', con=connection)
    df_queue.to_sql('queue', con=connection)
    df_group.to_sql('group', con=connection)
    df_mngmnt.to_sql('mngmnt', con=connection)
    df_horoscope.to_sql('horoscope', con=connection)

print('Создание базы данных завершено')