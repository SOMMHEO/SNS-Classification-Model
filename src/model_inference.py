# --- 데이터 전처리 및 BERT model 학습 ---
import pandas as pd
import numpy as np

import re
import emoji
from konlpy.tag import Okt

from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments
from datasets import Dataset, DatasetDict
import torch


def tokenize_and_predict_batch(new_profile_data, new_media_data, category_labels):
    new_profile = new_profile_data[['acnt_id', 'acnt_nm', 'acnt_conn_yn', 'acnt_sub_nm', 'intro_txt']]
    new_media = new_media_data[['acnt_id', 'media_cn']]

    new = pd.merge(new_profile, new_media, on='acnt_id')

    def clean_text(text):
        if not isinstance(text, str):
            return ''
        
        text = emoji.replace_emoji(text, replace='')
        text = re.sub(r'[^가-힣a-zA-Z0-9\s]', '', text)
        text = text.lower()
        text = re.sub(r'\s+', ' ', text)

        return text.strip()

    new['acnt_sub_nm_cleaned'] = new['acnt_sub_nm'].apply(clean_text)
    new['intro_txt_cleaned'] = new['intro_txt'].apply(clean_text)
    new['media_cn_cleaned'] = new['media_cn'].apply(clean_text)
    new = new[new['acnt_sub_nm_cleaned'].ne('') & new['intro_txt_cleaned'].ne('') & new['media_cn_cleaned'].ne('')]
    
    # 실험용으로 잠시 head(300)
    predict_df = new[['acnt_sub_nm_cleaned', 'intro_txt_cleaned', 'media_cn_cleaned']]

    # 학습된 BERT 모델 설정
    MODEL_NAME = "kykim/bert-kor-base" 
    FINETUNED_BERT_MODEL_PATH = "muli-columns-kykim-bert-kor" 

    # BERT 모델 및 토크나이저 로드 
    bert_tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    bert_model = AutoModelForSequenceClassification.from_pretrained(
        FINETUNED_BERT_MODEL_PATH,
        num_labels=len(category_labels)
    )
    bert_model.eval() # 추론 모드로 전환

    def tokenize_three_columns(examples):
        combined_texts = [
            f"{acnt} {bert_tokenizer.sep_token} {intro} {bert_tokenizer.sep_token} {txt}"
            for acnt, intro, txt in zip(
                examples["acnt_sub_nm_cleaned"],
                examples["intro_txt_cleaned"],
                examples["media_cn_cleaned"]
            )
        ]
        
        return bert_tokenizer(
            combined_texts,
            padding="max_length",
            truncation=True,
            max_length=512 
        )

    predict_dataset = Dataset.from_pandas(predict_df)
    predict_dataset = predict_dataset.map(tokenize_three_columns, batched=True)
    columns_to_remove = ['acnt_sub_nm_cleaned', 'intro_txt_cleaned', 'media_cn_cleaned']
    predict_dataset = predict_dataset.remove_columns(columns_to_remove)
    predict_dataset.set_format(type="torch", columns=['input_ids', 'attention_mask']) # 모델에 따라서 해당 부분 변경
    
    # 예측용 TrainingArguments 및 Trainer 설정
    prediction_args = TrainingArguments(
        output_dir="./prediction_output",
        per_device_eval_batch_size=16,
        do_train=False,
        do_predict=True,
        report_to="none",
        disable_tqdm=False,
    )
    trainer = Trainer(model=bert_model, args=prediction_args)

    # 예측 수행
    predictions_output = trainer.predict(predict_dataset)
    logits = predictions_output.predictions
    probabilities = torch.softmax(torch.tensor(logits), dim=-1).numpy()
    predicted_class_indices = np.argmax(logits, axis=-1)
    
    # 결과 DataFrame에 추가
    predict_df['bert_probabilities'] = [probs.tolist() for probs in probabilities]
    predict_df['bert_top_label_idx'] = np.argmax(probabilities, axis=-1)
    predict_df['bert_top_label'] = [category_labels[idx] for idx in predict_df['bert_top_label_idx']]
    predict_df['bert_top_prob'] = np.max(probabilities, axis=-1)
    
    return new_2, predict_df


