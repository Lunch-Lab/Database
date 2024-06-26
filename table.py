from sqlalchemy import create_engine, Column, String, Integer, MetaData, ForeignKey, Text, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


# 테이블 정의(클래스를 이용해 데이터베이스내 테이블의 구조(제약조건, 타입 등)를 가져옵니다.)
Base = declarative_base()

class Category(Base):
    # 업태구분 테이블
    __tablename__ = 'category'
    seq = Column(Integer, primary_key=True)
    type_code = Column(Integer)
    clf = Column(String)
    sub_clf = Column(String)

class tag_info(Base):
    # 태그정보 테이블
    __tablename__ = 'tag_info'
    seq = Column(Integer, primary_key=True)
    tag = Column(String)

class rest(Base):
    # 식당 테이블
    __tablename__ = 'rest'
    seq = Column(Integer, primary_key=True)
    cate_seq = Column(Integer, ForeignKey('category.seq'))
    rest_name = Column(String)
    road_address = Column(String)
    rest_name = Column(String)
    price = Column(Integer)
    vs_cnt = Column(Integer) 
    business_hour = Column(String)
    search_term = Column(String)   

class rest_re(Base):
    # 리뷰 테이블
    __tablename__ = 'rest_re'
    seq = Column(Integer, primary_key=True)
    rest_seq = Column(Integer, ForeignKey('rest.seq'))
    user_id = Column(String)
    review = Column(Text)
    sub_info = Column(String)

class tag(Base):
    # 태그 테이블
    __tablename__ = 'tag'
    seq = Column(Integer, primary_key=True)
    tag_seq = Column(Integer, ForeignKey('tag_info.seq'))
    rest_seq = Column(Integer, ForeignKey('rest.seq'))
    tag = Column(String)
    tag_cnt = Column(Integer)


class user(Base):
    # 사용자 테이블
    __tablename__ = 'user'
    seq = Column(Integer, primary_key=True)
    user_id = Column(String)
    url = Column(String)


# 네이버 마이플레이스 테이블....(추가해야해요...)