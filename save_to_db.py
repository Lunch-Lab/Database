# 데이터 베이스에 저장하기 위한 코드입니다.

# pip install psyopg2
# pip install python-dotenv psycopg2
# pip install sqlalchemy psycopg2

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, String, Integer, MetaData, ForeignKey, Text, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import psycopg2
import requests
import pandas as pd
from io import StirngIO
import time
import table


# 데이터베이스 연결 초기화 모델
def connect_db(host, user, password, database):
    connection =  f"postgresql+psycopg2://{my_user}:{my_password}@{my_host}:{my_port}/{my_dbname}"
    engine = create_engine(connection)
    print('연결 성공')
    return engine

# github api 저장소 연결 모델
def connect_github_api(repo_owner, repo_name, folder_path, branch):
    api_url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{folder_path}?ref={branch}'
    response = requests.get(api_url)
    file_urls = []
    if response.status_code == 200:
        files = response.json()
        for file in files:
            if file['type'] == 'file' and file['name'].endswith('.xlsx'):
                file_url = file['download_url']
                file_urls.append(file_url)
        return file_urls
    
    else:
        return ('github 저장소 연결실패')



#-------------------------------------------------

# 환경변수로드
load_dotenv()


# 환경변수에서 값 가져오기
my_host = os.getenv('host')
my_user = os.getenv('user')
my_password = os.getenv('password')
my_dbname = os.getenv('database')


# 데이터베이스 연결
engine = connect_db(my_host,my_user,my_password,my_dbname)
start = time.time()

# 연결 세션
Session = sessionmaker(bind=engine)

with Session() as session:
    # github api 사용하여 폴더 내 데이터 가져오기
    repo_owner = ''
    repo_owner = ''
    folder_path = ''
    branch = ''
    file_urls = connect_github_api(repo_owner, repo_owner, folder_path, branch )

    for url in file_urls:    
        file_response = requests.get(url)
        if file_response.status_code == 200:
            data = file_response.content
            file_name = url.split('/')[-1]
            print(f'{file_name} 파싱 성공')

            # 데이터 파싱
            df = pd.read_excel(StirngIO(data.decode('utf-8')))
            
            # 데이터 구분은 칼럼명으로 했습니다.
            # 중복된 데이터인지 여부 확인 후, 존재하지 않는 데이터이면 데이터 일괄 삽입
            # 업태구분 테이블
            if all(col in df.columns for col in ['업태구분', '대분류', '세부분류']):
                for row in df.itertuples(index=False):
                    category_insert_query = table.Category.insert().values(
                        type_code =  int(row[1]),
                        clf = row[2],
                        sub_clf = row[3]
                    )
                    existing_data = session.query(table.Category).filter_by(type_code = row[1], clf=row[2], sub_clf=row[3]).first()
                    if existing_data is None:
                        category_row = session.execute(category_insert_query)
                        session.commit()
                    
                    else:
                        print('존재하는 데이터입니다.')

            
            # 태그정보 테이블
            elif all(col in df.columns for col in ['태그']):
                for row in df.itertuples(index=False):
                    taginfo_insert_query = table.tag_info.insert().values(
                        tag = row[1]
                    )
                    existing_data = session.query(table.tag_info).filter_by(tag = row[1]).first()
                    if existing_data is None:
                        category_row = session.execute(taginfo_insert_query)
                        session.commit()
                    
                    else:
                        print('존재하는 데이터입니다.')


            # 식당 테이블
            elif all(col in df.columns for col in ['식당이름' '업태구분', '주소', '영업시간', '메뉴', 
                                                   '가격', '방문자리뷰',	'블로그리뷰', '검색어']):
                for row in df.itertuples(index=False):
                    # 업태구분 -> category.seq 참조
                    cate_seq = select(table.Category.c.seq).where(table.Category.c.sub_clf == row[2])

                    # 영업시간 전처리해야함....(함수 생성할 것...)
                    rest_insert_query = table.rest.insert().values(
                        cate_seq = cate_seq,
                        rest_name = row[1],
                        road_address = row[3],
                        # business_hour = row[4] 전처리 필요
                        rest_menu = row[5],
                        # price 전처리 필요 : price는 '원' 제외시키기, 
                        price = int(row[6]),
                        vs_cnt = int(row[7])
                    )

                    existing_data = session.query(table.rest).filter_by().first()
                    if existing_data is None:
                        category_row = session.execute(rest_insert_query)
                        session.commit()
                        
                    else:
                        print('존재하는 데이터입니다.')

            # 식당 태그 테이블
            elif all(col in df.columns for col in ['매장명', 'tag', '인원']):
                for row in df.itertuples(index=False):
                    # tag_seq는 태그 테이블(tag.seq)에서 참조. 
                    tag_seq = select(table.tag_info.seq).where(table.tag==row[2])
                    # rest_seq는 식당 테이블(rest.seq)에서 참조.
                    rest_seq = select(table.rest.seq).where(table.rest.rest_name == row[1])

                    tag_insert_query = table.tag.insert().values(
                        tag_seq = tag_seq,
                        rest_seq = rest_seq,
                        tag_cnt = row[3]
                    )

                    existing_data = session.query(table.tag).filter_by(tag_seq=tag_seq, rest_seq=rest_seq, tag_cnt=row[3]).first()
                    if existing_data is None:
                        category_row = session.execute(rest_insert_query)
                        session.commit()
                        
                    else:
                        print('존재하는 데이터입니다.')

            # 식당 리뷰 테이블
            elif all(col in df.columns for col in ['검색어', 'user_id', 'review', 'date', 'revisit', 'sub_info']):
                for row in df.itertuples(index=False):
                    # rest_seq는 식당 테이블(rest.seq)에서 참조.
                    rest_seq = select(table.rest.seq).where(table.rest.search_term == row[1])

                    rest_re_insert_query = table.rest_re.insert().values(
                        rest_seq = rest_seq,
                        user_id = row[2],
                        review = row[3],
                        sub_info = row[6]
                    )

                    existing_data = session.query(table.rest_re).filter_by(rest_seq=rest_seq, user_id=row[2], review=row[3]).first()
                    if existing_data is None:
                        category_row = session.execute(rest_re_insert_query)
                        session.commit()
                        
                    else:
                        print('존재하는 데이터입니다.')

            # 유저 테이블
            elif all(col in df.columns for col in ['검색어', 'user_id', 'url']):
                for row in df.itertuples(index=False):
                    user_insert_query = table.user.insert().values(
                        user_id = row[2],
                        url = row[3]
                    )
                
                    existing_data = session.query(table.user).filter_by(user_id = row[2], url = row[3]).first()
                    if existing_data is None:
                        category_row = session.execute(user_insert_query)
                        session.commit()
                        
                    else:
                        print('존재하는 데이터입니다.')

            # 마이플레이스 유저 테이블(만들어야 합니다...)

            else:
                print('주어진 칼럼에 해당하는 데이터가 아닙니다.')


end = time.time()
print('데이터베이스 저장에 걸린 시간:', end-start)

# 연결 종료
session.close()


