
from dotenv import load_dotenv
import os
import boto3
from io import BytesIO

import pandas as pd
import numpy as np
import json

from DB_connection import *
from model_inference import *

from paramiko import RSAKey
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from datetime import datetime

def main():
    # S3 Data Load
    load_dotenv("config/.env")

    aws_access_key = os.getenv("aws_accessKey")
    aws_secret_key = os.getenv("aws_secretKey")
    bucket_name = 'flexmatch-data'
    region_name = 'ap-northeast-2'

    s3 = boto3.client('s3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=region_name)

    
    today = datetime.now()
    year, week, _ = today.isocalendar()

    # profile_data_prefix = f'instagram-data/tables/CONN_v2_RECENT_USER_INFO_MTR/{today}/'
    # media_data_prefix = f'instagram-data/tables/CONN_v2_BY_USER_ID_MEDIA_DTL_INFO/{today}/'

    # # profile 파일 목록
    # profile_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=profile_data_prefix)
    # profile_keys = [obj['Key'] for obj in profile_response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    # # media 파일 목록
    # media_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=media_data_prefix)
    # media_keys = [obj['Key'] for obj in media_response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    # profile_dfs = []

    # for key in profile_keys:
    #     obj = s3.get_object(Bucket=bucket_name, Key=key)
    #     body = obj['Body'].read()
    #     df = pd.read_parquet(BytesIO(body))
    #     profile_dfs.append(df)

    # new_profile_data = pd.concat(profile_dfs, ignore_index=True)

    # media_dfs = []

    # for key in media_keys:
    #     obj = s3.get_object(Bucket=bucket_name, Key=key)
    #     body = obj['Body'].read()
    #     df = pd.read_parquet(BytesIO(body))
    #     media_dfs.append(df)

    # new_media_data = pd.concat(media_dfs, ignore_index=True)

    profile_prefixes = [
        # f'instagram-data/tables/EXTERNAL_RECENT_USER_INFO_MTR/year={year}/week={week}',
        # f'instagram-data/tables/EXTERNAL_2_RECENT_USER_INFO_MTR/year={year}/week={week}',
        f'instagram-data/tables/RECENT_USER_INFO_MTR/year={year}/week={week}/'
        # f'instagram-data/tables/CONN_v2_RECENT_USER_INFO_MTR/'
    ]

    media_prefixes = [
        # f'instagram-data/tables/EXTERNAL_BY_USER_ID_MEDIA_DTL_INFO/year={year}/week={week}',
        # f'instagram-data/tables/EXTERNAL_2_BY_USER_ID_MEDIA_DTL_INFO/year={year}/week={week}',
        f'instagram-data/tables/BY_USER_ID_MEDIA_DTL_INFO/year={year}/week={week}/'
        # f'instagram-data/tables/CONN_v2_BY_USER_ID_MEDIA_DTL_INFO/2025-08-14/'
    ]

    # 모든 파일을 저장할 리스트
    all_profile_dfs = []
    all_media_dfs = []

    # 함수를 사용하여 코드 중복 제거
    def get_s3_files(prefix_list, df_list):
        for prefix in prefix_list:
            print(f"Processing prefix: {prefix}")
            try:
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
                keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

                if not keys:
                    print(f"No .parquet files found in {prefix}")
                    continue
                
                for key in keys:
                    try:
                        obj = s3.get_object(Bucket=bucket_name, Key=key)
                        df = pd.read_parquet(BytesIO(obj['Body'].read()))
                        df_list.append(df)
                    except Exception as e:
                        print(f"Error reading file {key}: {e}")
            except Exception as e:
                print(f"Error listing objects for prefix {prefix}: {e}")

    get_s3_files(profile_prefixes, all_profile_dfs)
    get_s3_files(media_prefixes, all_media_dfs)

    if all_profile_dfs:
        not_conn_user_info_mtr = pd.concat(all_profile_dfs, ignore_index=True)
        print("/nFinal Combined Profile Data Info:")
        print(not_conn_user_info_mtr.info())
    else:
        not_conn_user_info_mtr = pd.DataFrame() # 빈 데이터프레임 생성
        print("/nNo profile data found.")

    if all_media_dfs:
        not_conn_user_media_info = pd.concat(all_media_dfs, ignore_index=True)
        print("/nFinal Combined Media Data Info:")
        print(not_conn_user_media_info.info())
    else:
        not_conn_user_media_info = pd.DataFrame() # 빈 데이터프레임 생성
        print("/nNo media data found.")

    ## merge data with DB data - only flexmatch influencer
    # preprocessing DB data
    flexmatch_influencer_info, conn_user_info_mtr, conn_user_media_info = get_all_infos()
    flexmatch_influencer_info['member_uid'] = flexmatch_influencer_info['member_uid'].fillna(0).astype(int)
    flexmatch_influencer_info['add1'] = flexmatch_influencer_info['add1'].str.replace('https://www.instagram.com/', '')
    flexmatch_influencer_info['acnt_nm'] = flexmatch_influencer_info['add1'].str.replace('/', '')

    # merge not_conn_user and conn_user
    new_profile_data = pd.concat([not_conn_user_info_mtr, conn_user_info_mtr], axis=0)
    new_media_data = pd.concat([not_conn_user_media_info, conn_user_media_info], axis=0)

    # category labeling
    category_labels = ['IT', '게임', '결혼/연애', '교육', '다이어트/건강보조식품', '만화/애니/툰', '문구/완구', '미술/디자인', '반려동물', '베이비/키즈', '뷰티', '브랜드공식계정',
                    '사진/영상', '셀럽', '스포츠', '시사', '엔터테인먼트', '여행/관광', '유명장소/핫플', '일상', '자동차/모빌리티', '짤/밈', '취미', '패션', '푸드', '홈/리빙']

    merged_df, new_merged_df, predict_df = tokenize_and_predict_batch(new_profile_data, new_media_data, category_labels)
    new_merged_df = new_merged_df[['acnt_id', 'acnt_nm']].reset_index(drop=True)
    predict_df.new_merged_df(drop=True, inplace=True)
    
    # final data after category labeling
    final_predict_df = pd.concat([new_merged_df, predict_df], axis=1)
    # final_predict_df.to_csv("flexmatch_influencer_category_matching.csv")  # 확인

    ## 알고리즘으로 카테고리 라벨링된 사람들
    db_merge_df = pd.merge(final_predict_df, flexmatch_influencer_info, on='acnt_nm', how='left')

    main_category = db_merge_df.groupby(['acnt_id', 'acnt_nm'])['bert_top_label'].agg(lambda x: x.value_counts().idxmax()).to_frame().reset_index().rename(columns={'bert_top_label' : 'main_category'})

    top_3_labels = db_merge_df.groupby(['acnt_id', 'acnt_nm'])['bert_top_label'].value_counts().groupby(level=[0,1]).head(3).reset_index(name='count')
    top_3_labels_joined = (top_3_labels.groupby(['acnt_id', 'acnt_nm'])['bert_top_label'].apply(lambda x: '@'.join(x)).reset_index(name='top_3_category'))
    
    top_3_labels_joined = top_3_labels_joined.drop(columns=['acnt_id', 'acnt_nm'])
    final_df = pd.concat([main_category, top_3_labels_joined], axis=1)

    ## 알고리즘으로 카테고리 라벨링 안된 사람들(게시물 자체가 없는 사람과 게시물이 있어도 글이 없어서 라벨링 안된 사람들)
    final_df_list = final_df['acnt_id'].drop_duplicates().to_list()
    no_category_user_df = merged_df[~merged_df['acnt_id'].isin(final_df_list)].drop_duplicates("acnt_id")[['acnt_id', 'acnt_nm']].reset_index(drop=True)

    # 여기서 게시물 자체가 없는 사람들은 제외하고 태깅
    has_media_user_list = new_media_data['acnt_id'].unique()
    no_post_user_list = merged_df[~merged_df['acnt_id'].isin(has_media_user_list)]['acnt_id'].to_list()
    final_no_category_user_df = no_category_user_df[~no_category_user_df['acnt_id'].isin(no_post_user_list)]

    final_no_category_user_df['main_category'] = '일상'
    final_no_category_user_df['top_3_category'] = None

    # DB Insert
    data_list = final_df.to_dict(orient='records')
    
    ssh = SSHMySQLConnector()
    ssh.load_config_from_json('C:/Users/flexmatch/Desktop/ssom/code/3.SNS-categorizer/config/ssh_db_config.json') 
    ssh.connect(True)
    ssh.insert_query_with_lookup('INSTAGRAM_USER_CATEGORY_LABELING', data_list=data_list)

    ssh.close()

if __name__=='__main__':
    main()