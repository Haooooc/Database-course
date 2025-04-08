import sqlite3
import pymongo

# 连接到 SQLite 数据库
sqlite_conn = sqlite3.connect(r'D:\BaiduNetdiskDownload\book_lx.db')
sqlite_cursor = sqlite_conn.cursor()

# 连接到 MongoDB
mongo_client = pymongo.MongoClient('mongodb://localhost:27017')
mongo_db = mongo_client['bookstore']

# 查询 SQLite 数据
sqlite_cursor.execute('SELECT * FROM book')
rows = sqlite_cursor.fetchall()

if not rows:
    print("No data found in the SQLite database.")
else:
    # 将数据插入到 MongoDB
    inserted_count = 0
    for row in rows:
        document = {
            'id': row[0],
            'title': row[1],
            'author': row[2],
            'publisher': row[3],
            'original_title': row[4],
            'translator': row[5],
            'pub_year': row[6],
            'pages': row[7],  # 确保类型适合
            'price': row[8],  # 确保类型适合
            'currency_unit': row[9],
            'binding': row[10],
            'isbn': row[11],
            'author_intro': row[12],
            'book_intro': row[13],
            'content': row[14],
            'tags': row[15],
            'picture': row[16],
        }
        
        try:
            result = mongo_db.books.insert_one(document)
            if result.acknowledged:
                inserted_count += 1
                print(f"Inserted document with ID {result.inserted_id}")
            else:
                print(f"Failed to insert document: {document}")
        except Exception as e:
            print(f"Error inserting document: {e}, Document: {document}")

    if inserted_count == 0:
        print("No documents were inserted into MongoDB.")
    else:
        print(f"{inserted_count} documents were successfully inserted into MongoDB.")

# 关闭连接
sqlite_conn.close()
mongo_client.close()