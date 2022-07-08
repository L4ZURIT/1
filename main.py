from models import Book, Publisher, Author
from db_controller import DBM

def main():

    sqlm = DBM()

    #Создаем новую запись.
    new_author = Author(
        first_name = "Steve",
        last_name = "King"
    )
    # Добавляем запись
    sqlm.session.add(new_author)

    #Закрепляем запись
    sqlm.session.commit()

    #А теперь попробуем вывести всех авторов, которые есть в нашей таблице
    for auth in sqlm.session.query(Author):
        print(auth.author_id)
        print(auth.first_name)
        print(auth.last_name)

    pass



if __name__ == '__main__':
    main()