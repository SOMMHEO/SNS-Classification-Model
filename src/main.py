
from dotenv import load_dotenv
import os
import boto3
from io import BytesIO

import pandas as pd
import numpy as np

def main():
    # S3 Data Load
    load_dotenv()
    aws_access_key = os.getenv("aws_accessKey")
    aws_secret_key = os.getenv("aws_secretKey")
    bucket_name = 'flexmatch-data'
    region_name = 'ap-northeast-2'

    s3 = boto3.client('s3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=region_name)
    
    # today = datetime.now().strftime('%Y-%m-%d')
    # year, week, _ = today.isocalendar()
    
    # profile_data_prefix = f'instagram-data/tables/EXTERNAL_2_RECENT_USER_INFO_MTR/year={year}/week={week}'
    # media_data_prefix = f'instagram-data/tables/EXTERNAL_2_BY_USER_ID_MEDIA_DTL_INFO/year={year}/week={week}'

    profile_data_prefix = f'instagram-data/tables/EXTERNAL_2_RECENT_USER_INFO_MTR/year=2025/week=30/'
    media_data_prefix = f'instagram-data/tables/EXTERNAL_2_BY_USER_ID_MEDIA_DTL_INFO/year=2025/week=30/'

    # profile 파일 목록
    profile_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=profile_data_prefix)
    profile_keys = [obj['Key'] for obj in profile_response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    # media 파일 목록
    media_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=media_data_prefix)
    media_keys = [obj['Key'] for obj in media_response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    profile_dfs = []

    for key in profile_keys:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        body = obj['Body'].read()
        df = pd.read_parquet(BytesIO(body))
        profile_dfs.append(df)

    new_profile_data = pd.concat(profile_dfs, ignore_index=True)

    media_dfs = []

    for key in media_keys:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        body = obj['Body'].read()
        df = pd.read_parquet(BytesIO(body))
        media_dfs.append(df)

    new_media_data = pd.concat(media_dfs, ignore_index=True)

    # category labeling
    category_labels = ['IT', '게임', '결혼/연애', '교육', '다이어트/건강보조식품', '만화/애니/툰', '문구/완구', '미술/디자인', '반려동물', '베이비/키즈', '뷰티', '브랜드공식계정',
                    '사진/영상', '셀럽', '스포츠', '시사', '엔터테인먼트', '여행/관광', '유명장소/핫플', '일상', '자동차/모빌리티', '짤/밈', '취미', '패션', '푸드', '홈/리빙']

    merged_df, predict_df = tokenize_and_predict_batch(new_profile_data, new_media_data, category_labels)
    merged_df = merged_df[['acnt_id', 'acnt_nm', 'acnt_conn_yn']].reset_index(drop=True)
    predict_df.reset_index(drop=True, inplace=True)
    
    final_predict_df = pd.concat([merged_df, predict_df], axis=1)
    final_predict_df.to_excel("real_category_matching.xlsx")

    main_category = final_predict_df.groupby(['acnt_id', 'acnt_nm', 'acnt_conn_yn_x'])['single_label'].agg(lambda x: x.value_counts().idxmax()).to_frame().reset_index().rename(columns={'single_label' : 'main_category'})

    top_3_labels = final_predict_df.groupby(['acnt_id', 'acnt_nm', 'acnt_conn_yn_x'])['single_label'].value_counts().groupby(level=[0,1]).head(3).reset_index(name='count')
    top_3_labels_joined = (top_3_labels.groupby(['acnt_id', 'acnt_nm', 'acnt_conn_yn_x'])['single_label'].apply(lambda x: '@'.join(x)).reset_index(name='top_3_category'))
    
    top_3_labels_joined = top_3_labels_joined.drop(columns=['acnt_id', 'acnt_nm', 'acnt_conn_yn_x'])
    final_df = pd.concat([main_category, top_3_labels_joined], axis=1)
    final_df.rename(columns={'acnt_conn_yn_x' : 'is_conntected'})

    # DB Insert
    

if __name__=='__main__':
    main()