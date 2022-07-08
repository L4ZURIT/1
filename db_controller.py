# Импортируем модели для работы с бд
from models import Book, Publisher, Author

import pandas as pd

from sqlalchemy import create_engine, Column, Table, MetaData, ForeignKey, select
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, Query, scoped_session
from sqlalchemy.exc import ResourceClosedError

class DBM():
    

    connection_info = {

    }

    def __init__(self) -> None:
        self.engine = create_engine("sqlite:///database.db", pool_recycle=1800)
        self.md = MetaData(bind=self.engine)
        self.dialect = None
        self.db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=self.engine))
        Session = sessionmaker(bind=self.engine)
    # Создаем объект сессии из вышесозданной фабрики Session
        self.session = Session()

    # Передает ручной запрос
    def request(self, req):
        conn = self.engine.connect()
        
        try:
            result = conn.execute(req)
        except Exception as ex:
            return ex
        try:
            return "\n".join([str(res) for res in result])
        except ResourceClosedError:
            return "no_data"

    def get_table(self, table:Table):
        req = table.select()
        conn = self.engine.connect()
        
        #tab = pd.read_sql(str(req.compile(self.engine)), conn)
        res = conn.execute(req)
        tab = pd.DataFrame(res.fetchall())
        ans = tab.to_dict(orient='list')
        if ans == {}:
            ans = {str(col.name):[] for col in table.columns}
        return ans
        
    def get_column(self, table, column):
        req = table.select()
        conn = self.engine.connect()
        
        #tab = pd.read_sql(str(req.compile(self.engine)), conn)
        res = conn.execute(req)
        tab = pd.DataFrame(res.fetchall())
        ans = tab.to_dict(orient='list')
        if ans == {}:
            ans = {str(col.name):[] for col in table.columns}
        return ans[column]
