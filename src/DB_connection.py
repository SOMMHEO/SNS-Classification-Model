import pymysql
from datetime import datetime, timedelta
from sshtunnel import SSHTunnelForwarder
import json
import pandas as pd

class SSHMySQLConnector:
    def __init__(self):
        self.ssh_host = None
        self.ssh_username = None
        self.ssh_password = None
        self.db_username = None
        self.db_password = None
        self.db_name = None
        self.tunnel = None
        self.connection = None

    def load_config_from_json(self, json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.ssh_host = config['ssh_host']
                self.ssh_username = config['ssh_username']
                self.ssh_password = config['ssh_password']
                self.db_username = config['db_username']
                self.db_password = config['db_password']
                self.db_name = config['db_name']
        except Exception as e:
            print("설정 JSON 로딩 실패:", e)

    def connect(self, insert=False):
        try:
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_host, 22),
                ssh_username=self.ssh_username,
                ssh_password=self.ssh_password,
                remote_bind_address=('127.0.0.1', 3306),
            )
            self.tunnel.start()
            # insert 여부에 따라 cursorclass 설정
            connect_kwargs = {
                'host': '127.0.0.1',
                'port': self.tunnel.local_bind_port,
                'user': self.db_username,
                'password': self.db_password,
                'db': self.db_name,
            }
            if insert:
                connect_kwargs['cursorclass'] = pymysql.cursors.DictCursor
            self.connection = pymysql.connect(**connect_kwargs)
            print("DB 접속 성공")
        except Exception as e:
            print("SSH 또는 DB 연결 실패:", e)

    def execute_query(self, query):
        # 쿼리 실행 후 데이터를 DataFrame으로 반환
        return pd.read_sql_query(query, self.connection)

    # Data insert after category matching
    def insert_query_with_lookup(self, table_name, data_list):
        try:
            with self.connection.cursor() as cursor:
                for data in data_list:
                    # 1. op_member에서 uid, user_id 조회
                    cursor.execute("""
                        SELECT uid, user_id, add1_connected FROM op_member
                        WHERE add1 = %s
                        LIMIT 1
                    """, (data['acnt_nm'],))
                    result = cursor.fetchone()
                    
                    if result:
                        data['member_uid'] = result['uid']
                        # data['user_id'] = result['user_id']
                        data['is_connected'] = result['add1_connected']
                        # 향후에 ig_user_id가 추가가 된다면, 해당 부분도 확인해서 추가할 수 있게
                        # data['ig_user_id'] = result['ig_user_id']
                    else:
                        data['member_uid'] = 0
                        # data['user_id'] = 'None'
                        data['is_connected'] = 'n'
                        # data['ig_user_id'] = 'None'

                    # 2. INSERT 쿼리 구성 및 실행
                    columns = ', '.join(data.keys())
                    placeholders = ', '.join([f"%({k})s" for k in data.keys()])                    
                    # insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    # 수정 (해당 값이 이미 있으면 제외하고 insert)
                    update_clause = ', '.join([f"{k} = VALUES({k})" for k in data.keys()])
                    insert_sql = f"""
                        INSERT INTO {table_name} ({columns}) 
                        VALUES ({placeholders})
                        ON DUPLICATE KEY UPDATE {update_clause}
                    """
                    cursor.execute(insert_sql, data)

                    if cursor.rowcount == 1:
                        print(f"✅ inserted new: {data.get('acnt_id', 'N/A')}")
                    else:
                        print(f"🔁 updated existing: {data.get('acnt_id', 'N/A')}")

                    # print(f"inserted acnt_id: {data.get('acnt_id', 'N/A')}")

            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print("INSERT 실패:", e)
    
    def close(self):
        if self.connection:
            self.connection.close()
        if self.tunnel:
            self.tunnel.stop()

def sendQuery(query):
        ssh = SSHMySQLConnector()
        ssh.load_config_from_json('C:/Users/flexmatch/Desktop/ssom/code/3.SNS-categorizer/config/ssh_db_config.json')
        ssh.connect()
        results = ssh.execute_query(query)
        # print(results)
        # print(results.head())
        ssh.close()

        return results


## DB data loading for merge data
def get_all_infos(): 

    query_flexmatch_member_info = """
        select DISTINCT
        o.user_id, s.member_uid, o.add1
        from op_member o
        left join op_mem_seller_statistics s on o.user_id=s.user_id
        where o.add1 is not null and o.add1 != ''
    """
    flexmatch_influencer_info = sendQuery(query_flexmatch_member_info)

    query_s3_conn_user_info_mtr = """
        select acnt_id, acnt_nm, web_addr, acnt_sub_nm, intro_txt, profile_photo_url_addr, acnt_conn_yn, category_nm, follower_cnt, follow_cnt, media_cnt
        from S3_CONN_v2_RECENT_USER_INFO_MTR
    """
    conn_user_info_mtr = sendQuery(query_s3_conn_user_info_mtr)

    query_s3_conn_user_media_info_mtr = """
        select acnt_id, media_id, media_type_nm, reels_feed_type_nm, media_url_addr, media_unq_url_addr, tmnl_url_addr, reg_dt, media_cn, acnt_conn_yn, feed_share_yn, cmnt_actvtn_yn
        from S3_CONN_v2_BY_USER_ID_MEDIA_DTL_INFO
    """
    conn_user_media_info = sendQuery(query_s3_conn_user_media_info_mtr)
    
    return flexmatch_influencer_info, conn_user_info_mtr, conn_user_media_info


